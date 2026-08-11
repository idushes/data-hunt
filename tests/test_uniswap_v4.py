import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.uniswap_v4 import (
    Q128_INT,
    UNISWAP_V4_CHAINS,
    _claimable_fees_raw,
    _discover_token_ids,
    _position_ticks,
    _render_v4_csv,
    get_uniswap_v4_positions_csv,
)


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"


class UniswapV4StateTest(unittest.TestCase):
    def test_decodes_signed_ticks_from_packed_position_info(self):
        tick_lower = -120
        tick_upper = 240
        info = ((tick_lower % (1 << 24)) << 8) | (
            (tick_upper % (1 << 24)) << 32
        )

        self.assertEqual(_position_ticks(info), (tick_lower, tick_upper))

    def test_calculates_claimable_fees_from_fee_growth_inside(self):
        state = {
            "current_tick": 0,
            "liquidity": 100,
            "fee_growth_global0": 5 * Q128_INT,
            "fee_growth_global1": 9 * Q128_INT,
            "lower_outside0": Q128_INT,
            "lower_outside1": 2 * Q128_INT,
            "upper_outside0": Q128_INT,
            "upper_outside1": 3 * Q128_INT,
            "fee_growth_last0": Q128_INT,
            "fee_growth_last1": 2 * Q128_INT,
        }

        self.assertEqual(_claimable_fees_raw(state, -10, 10), (200, 200))

    def test_uses_official_mainnet_deployments(self):
        ethereum = UNISWAP_V4_CHAINS[1]
        self.assertEqual(
            ethereum["pool_manager"].lower(),
            "0x000000000004444c5dc75cb358380d2e3de08a90",
        )
        self.assertEqual(
            ethereum["position_manager"].lower(),
            "0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e",
        )
        self.assertEqual(set(UNISWAP_V4_CHAINS), {1, 8453, 42161})


class UniswapV4DiscoveryTest(unittest.IsolatedAsyncioTestCase):
    async def test_discovers_and_paginates_owned_nfts(self):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if len(requests) == 1:
                return httpx.Response(
                    200,
                    json={
                        "items": [{"id": "8"}, {"id": "3"}],
                        "next_page_params": {"unique_token": "3"},
                    },
                )
            return httpx.Response(
                200,
                json={"items": [{"id": "1"}], "next_page_params": None},
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            result = await _discover_token_ids(
                client,
                "https://blockscout.example/api/v2",
                "0x0000000000000000000000000000000000000001",
                WALLET,
            )

        self.assertEqual(result, [1, 3, 8])
        self.assertEqual(requests[0].url.params["holder_address_hash"], WALLET)
        self.assertEqual(requests[1].url.params["unique_token"], "3")

    async def test_rejects_unsupported_chain(self):
        with self.assertRaises(HTTPException) as context:
            from routers.uniswap_v4 import _fetch_uniswap_v4_rows

            await _fetch_uniswap_v4_rows(WALLET, 999999, False)

        self.assertEqual(context.exception.status_code, 400)


class UniswapV4CsvTest(unittest.IsolatedAsyncioTestCase):
    def test_renders_stable_position_id_and_claimable_fees(self):
        content = _render_v4_csv(
            [
                {
                    "wallet": WALLET,
                    "chain_id": "1",
                    "protocol": "Uniswap V4",
                    "position_id": "1:manager:144031",
                    "token_id": "144031",
                    "token0_fees_claimable": "0.001",
                    "token1_fees_claimable": "42",
                    "fees_value_usd": "105",
                }
            ]
        )
        parsed = list(csv.DictReader(io.StringIO(content)))

        self.assertEqual(parsed[0]["position_id"], "1:manager:144031")
        self.assertEqual(parsed[0]["token1_fees_claimable"], "42")
        self.assertEqual(parsed[0]["fees_value_usd"], "105")

    async def test_route_returns_csv(self):
        rows = [
            {
                "wallet": WALLET,
                "chain_id": "1",
                "position_id": "1:manager:144031",
                "token_id": "144031",
                "liquidity": "42",
            }
        ]
        with patch(
            "routers.uniswap_v4._fetch_uniswap_v4_rows",
            AsyncMock(return_value=rows),
        ) as fetch:
            response = await get_uniswap_v4_positions_csv(WALLET, 1, False)

        fetch.assert_awaited_once_with(WALLET, 1, False)
        self.assertEqual(response.media_type, "text/csv")
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")
        self.assertIn("1:manager:144031", response.body.decode())


if __name__ == "__main__":
    unittest.main()
