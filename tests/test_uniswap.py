import csv
import io
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException
from web3 import Web3

from routers.uniswap import (
    MAX_UINT128,
    MONAD_USD_STABLECOINS,
    POSITION_MANAGER_ABI,
    Q96,
    UNISWAP_CHAINS,
    _fetch_uniswap_rows,
    _human_price,
    _normalize_wallet,
    _position_amounts,
    _render_csv,
    _rpc_batch_calls,
    _usd_value,
    get_uniswap_positions_csv,
)


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"


class UniswapMathTest(unittest.TestCase):
    def test_price_at_one_to_one_sqrt_ratio(self):
        self.assertEqual(_human_price(int(Q96), 6, 6), Decimal("1"))

    def test_position_amounts_are_split_inside_range(self):
        amount0, amount1 = _position_amounts(
            liquidity=10**18,
            tick_lower=-100,
            tick_upper=100,
            sqrt_price_x96=int(Q96),
            decimals0=18,
            decimals1=18,
        )

        self.assertGreater(amount0, 0)
        self.assertGreater(amount1, 0)
        self.assertAlmostEqual(float(amount0), float(amount1), places=12)

    def test_usd_value_uses_stable_token_side(self):
        usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        wbtc = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

        self.assertEqual(
            _usd_value(wbtc, usdc, Decimal("0.5"), Decimal("100"), Decimal("60000")),
            Decimal("30100"),
        )
        self.assertAlmostEqual(
            float(
                _usd_value(
                    usdc,
                    wbtc,
                    Decimal("100"),
                    Decimal("0.5"),
                    Decimal("0.00001666666666666666666666666667"),
                )
            ),
            30100,
            places=8,
        )

    def test_usd_value_uses_monad_native_usdc(self):
        wmon = "0x3bd359c1119da7da1d913d1c4d2b7c461115433a"
        usdc = "0x754704bc059f8c67012fed69bc8a327a5aafb603"

        self.assertEqual(
            _usd_value(
                wmon,
                usdc,
                Decimal("100000"),
                Decimal("1000"),
                Decimal("0.02"),
                MONAD_USD_STABLECOINS,
            ),
            Decimal("3000"),
        )


class UniswapCsvTest(unittest.TestCase):
    def test_renders_stable_position_id_and_amounts(self):
        row = {
            "wallet": WALLET,
            "chain_id": "1",
            "chain": "Ethereum",
            "protocol": "Uniswap V3",
            "position_id": "1:0xc364:1165639",
            "token_id": "1165639",
            "token0_symbol": "WBTC",
            "token0_amount": "1.25",
            "token1_symbol": "USDC",
            "token1_amount": "70000",
            "value_usd": "145000",
        }

        content = _render_csv([row])
        parsed = list(csv.DictReader(io.StringIO(content)))

        self.assertEqual(parsed[0]["position_id"], "1:0xc364:1165639")
        self.assertEqual(parsed[0]["token0_amount"], "1.25")
        self.assertEqual(parsed[0]["value_usd"], "145000")


class UniswapValidationTest(unittest.IsolatedAsyncioTestCase):
    def test_monad_uses_official_uniswap_v3_deployment(self):
        chain = UNISWAP_CHAINS[143]

        self.assertEqual(chain["name"], "Monad")
        self.assertEqual(
            chain["position_manager"],
            "0x7197e214c0b767cfb76fb734ab638e2c192f4e53",
        )
        self.assertEqual(
            chain["factory"],
            "0x204faca1764b154221e35c0d20abb3c525710498",
        )

    def test_rejects_invalid_wallet(self):
        with self.assertRaises(HTTPException) as context:
            _normalize_wallet("not-an-address")

        self.assertEqual(context.exception.status_code, 400)

    async def test_collect_simulation_uses_wallet_as_sender(self):
        manager_address = UNISWAP_CHAINS[143]["position_manager"]
        manager = Web3().eth.contract(
            address=Web3.to_checksum_address(manager_address),
            abi=POSITION_MANAGER_ABI,
        )
        encoded_result = Web3().codec.encode(
            ["uint256", "uint256"], [4_165 * 10**18, 89 * 10**6]
        )
        response = httpx.Response(
            200,
            json=[
                {
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": f"0x{encoded_result.hex()}",
                }
            ],
            request=httpx.Request("POST", "https://rpc.example"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post.return_value = response

        result = await _rpc_batch_calls(
            client,
            "https://rpc.example",
            [
                (
                    manager_address,
                    manager.functions.collect(
                        (
                            21699,
                            Web3.to_checksum_address(WALLET),
                            MAX_UINT128,
                            MAX_UINT128,
                        )
                    ),
                )
            ],
            from_address=WALLET,
        )

        payload = client.post.await_args.kwargs["json"]
        self.assertEqual(
            payload[0]["params"][0]["from"], Web3.to_checksum_address(WALLET)
        )
        self.assertEqual(result[0], (4_165 * 10**18, 89 * 10**6))

    async def test_rejects_unsupported_chain(self):
        with self.assertRaises(HTTPException) as context:
            await _fetch_uniswap_rows(WALLET, 999999, False)

        self.assertEqual(context.exception.status_code, 400)

    async def test_route_returns_csv(self):
        rows = [
            {
                "wallet": WALLET,
                "chain_id": "1",
                "position_id": "1:manager:123",
                "token_id": "123",
                "liquidity": "42",
            }
        ]
        with patch(
            "routers.uniswap._fetch_uniswap_rows",
            AsyncMock(return_value=rows),
        ) as fetch:
            response = await get_uniswap_positions_csv(WALLET, 1, False)

        fetch.assert_awaited_once_with(WALLET, 1, False)
        self.assertEqual(response.media_type, "text/csv")
        self.assertIn("1:manager:123", response.body.decode())


if __name__ == "__main__":
    unittest.main()
