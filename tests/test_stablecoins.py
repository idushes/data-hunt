import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.stablecoins import (
    ETHEREUM_TOKENS,
    _fetch_ethereum_balances,
    _fetch_stablecoin_rows,
    _format_balance,
    _render_csv,
    _sum_solana_token_accounts,
    get_stablecoin_balances_csv,
)


EVM_WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"
SOLANA_WALLET = "11111111111111111111111111111111"


class StablecoinBalanceTest(unittest.IsolatedAsyncioTestCase):
    def test_formats_six_decimal_balances(self):
        self.assertEqual(_format_balance(1_234_567, 6), "1.234567")
        self.assertEqual(_format_balance(0, 6), "0")

    def test_sums_all_solana_token_accounts(self):
        result = {
            "value": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "tokenAmount": {
                                        "amount": "1200000",
                                        "decimals": 6,
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "tokenAmount": {
                                        "amount": "300000",
                                        "decimals": 6,
                                    }
                                }
                            }
                        }
                    }
                },
            ]
        }

        self.assertEqual(_sum_solana_token_accounts(result, 6), 1_500_000)

    async def test_reads_ethereum_balances_in_one_batch(self):
        response = httpx.Response(
            200,
            json=[
                {"jsonrpc": "2.0", "id": 0, "result": hex(12_345_678)},
                {"jsonrpc": "2.0", "id": 1, "result": "0x0"},
            ],
            request=httpx.Request("POST", "https://rpc.example"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post.return_value = response

        rows = await _fetch_ethereum_balances(client, EVM_WALLET)

        client.post.assert_awaited_once()
        self.assertEqual(len(rows), len(ETHEREUM_TOKENS))
        self.assertEqual(rows[0]["balance"], "12.345678")
        self.assertEqual(rows[1]["balance"], "0")

    async def test_requires_at_least_one_wallet(self):
        with self.assertRaises(HTTPException) as context:
            await _fetch_stablecoin_rows(None, None)

        self.assertEqual(context.exception.status_code, 400)

    async def test_fetches_both_networks_and_keeps_zero_rows(self):
        ethereum_rows = [{"balance_id": "ethereum-usdc", "balance": "0"}]
        solana_rows = [{"balance_id": "solana-usdt", "balance": "0"}]
        with (
            patch(
                "routers.stablecoins._fetch_ethereum_balances",
                AsyncMock(return_value=ethereum_rows),
            ),
            patch(
                "routers.stablecoins._fetch_solana_balances",
                AsyncMock(return_value=solana_rows),
            ),
        ):
            rows = await _fetch_stablecoin_rows(EVM_WALLET, SOLANA_WALLET)

        self.assertEqual(rows, [*ethereum_rows, *solana_rows])

    async def test_route_returns_csv(self):
        rows = [
            {
                "balance_id": f"ethereum:1:{EVM_WALLET}:USDC",
                "wallet": EVM_WALLET,
                "network": "Ethereum",
                "chain_id": "1",
                "token_symbol": "USDC",
                "token_name": "USD Coin",
                "token_address": ETHEREUM_TOKENS[0]["address"],
                "balance": "10.5",
                "decimals": "6",
            }
        ]
        with patch(
            "routers.stablecoins._fetch_stablecoin_rows",
            AsyncMock(return_value=rows),
        ) as fetch:
            response = await get_stablecoin_balances_csv(EVM_WALLET, None)

        fetch.assert_awaited_once_with(EVM_WALLET, None)
        parsed = list(csv.DictReader(io.StringIO(response.body.decode())))
        self.assertEqual(parsed[0]["balance"], "10.5")
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")

    def test_csv_key_is_stable_and_explicit(self):
        content = _render_csv(
            [
                {
                    "balance_id": f"solana:mainnet:{SOLANA_WALLET}:USDT",
                    "wallet": SOLANA_WALLET,
                    "network": "Solana",
                    "chain_id": "solana-mainnet",
                    "token_symbol": "USDT",
                    "token_name": "Tether USD",
                    "token_address": "mint",
                    "balance": "0",
                    "decimals": "6",
                }
            ]
        )

        row = next(csv.DictReader(io.StringIO(content)))
        self.assertEqual(
            row["balance_id"], f"solana:mainnet:{SOLANA_WALLET}:USDT"
        )


if __name__ == "__main__":
    unittest.main()
