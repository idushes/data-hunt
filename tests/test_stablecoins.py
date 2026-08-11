import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.stablecoins import (
    ETHEREUM_TOKENS,
    ARBITRUM_TOKENS,
    BASE_TOKENS,
    SOLANA_TOKENS,
    TRON_TOKENS,
    _fetch_evm_balances,
    _fetch_ethereum_balances,
    _fetch_solana_balances,
    _fetch_stablecoin_rows,
    _fetch_tron_balances,
    _format_balance,
    _normalize_evm_address,
    _render_csv,
    _split_wallets,
    _sum_solana_token_accounts,
    get_stablecoin_balances_csv,
)
from routers.solana import SPL_TOKEN_2022_PROGRAM_ID, SPL_TOKEN_PROGRAM_ID


EVM_WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"
SOLANA_WALLET = "11111111111111111111111111111111"


class StablecoinBalanceTest(unittest.IsolatedAsyncioTestCase):
    def test_exposes_only_unique_stablecoins_in_a_stable_order(self):
        expected = (
            (ETHEREUM_TOKENS, 17, ("USDC", "USDT")),
            (ARBITRUM_TOKENS, 15, ("USDC", "USDT")),
            (BASE_TOKENS, 16, ("USDC", "USDT")),
            (SOLANA_TOKENS, 17, ("USDC", "USDT")),
            (TRON_TOKENS, 6, ("USDT", "USDC")),
        )
        for tokens, count, leading_symbols in expected:
            symbols = tuple(token["symbol"] for token in tokens)
            self.assertEqual(len(tokens), count)
            self.assertEqual(symbols[:2], leading_symbols)
            self.assertEqual(len(symbols), len(set(symbols)))
            self.assertTrue(all(token["address"] for token in tokens))

    def test_uses_the_canonical_solana_token_2022_program(self):
        self.assertEqual(
            SPL_TOKEN_2022_PROGRAM_ID,
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        )

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
                {
                    "jsonrpc": "2.0",
                    "id": index,
                    "result": hex(12_345_678) if index == 0 else "0x0",
                }
                for index in range(len(ETHEREUM_TOKENS))
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
        self.assertEqual(
            rows[0]["balance_id"], f"ethereum:1:{EVM_WALLET}:USDC"
        )

    async def test_reads_arbitrum_balances_with_stable_ids(self):
        response = httpx.Response(
            200,
            json=[
                {
                    "jsonrpc": "2.0",
                    "id": index,
                    "result": hex(1_000_000) if index == 0 else "0x0",
                }
                for index in range(len(ARBITRUM_TOKENS))
            ],
            request=httpx.Request("POST", "https://rpc.example"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post.return_value = response

        rows = await _fetch_evm_balances(client, EVM_WALLET, 42161)

        self.assertEqual(len(rows), len(ARBITRUM_TOKENS))
        self.assertEqual(rows[0]["network"], "Arbitrum")
        self.assertEqual(
            rows[0]["balance_id"], f"evm:42161:{EVM_WALLET}:USDC"
        )

    async def test_reads_base_balances(self):
        response = httpx.Response(
            200,
            json=[
                {
                    "jsonrpc": "2.0",
                    "id": index,
                    "result": (
                        hex(2_000_000)
                        if index == 0
                        else hex(3_000_000) if index == 1 else "0x0"
                    ),
                }
                for index in range(len(BASE_TOKENS))
            ],
            request=httpx.Request("POST", "https://rpc.example"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post.return_value = response

        rows = await _fetch_evm_balances(client, EVM_WALLET, 8453)

        self.assertEqual(len(rows), len(BASE_TOKENS))
        self.assertEqual(rows[0]["network"], "Base")
        self.assertEqual(rows[0]["balance"], "2")
        self.assertEqual(rows[1]["balance"], "3")

    async def test_reads_all_solana_balances_in_two_rpc_requests(self):
        def token_account(mint: str, amount: str, decimals: int):
            return {
                "account": {
                    "data": {
                        "parsed": {
                            "info": {
                                "mint": mint,
                                "tokenAmount": {
                                    "amount": amount,
                                    "decimals": decimals,
                                },
                            }
                        }
                    }
                }
            }

        rpc = AsyncMock(
            side_effect=[
                {
                    "value": [
                        token_account(SOLANA_TOKENS[0]["address"], "2500000", 6)
                    ]
                },
                {
                    "value": [
                        token_account(SOLANA_TOKENS[2]["address"], "4000000", 6)
                    ]
                },
            ]
        )
        with patch("routers.stablecoins._solana_rpc_request", rpc):
            rows = await _fetch_solana_balances(AsyncMock(), SOLANA_WALLET)

        self.assertEqual(rpc.await_count, 2)
        self.assertEqual(len(rows), len(SOLANA_TOKENS))
        self.assertEqual(rows[0]["balance"], "2.5")
        self.assertEqual(rows[1]["balance"], "0")
        self.assertEqual(rows[2]["balance"], "4")
        program_ids = {
            call.args[2][1]["programId"] for call in rpc.await_args_list
        }
        self.assertEqual(
            program_ids,
            {SPL_TOKEN_PROGRAM_ID, SPL_TOKEN_2022_PROGRAM_ID},
        )

    async def test_requires_at_least_one_wallet(self):
        with self.assertRaises(HTTPException) as context:
            await _fetch_stablecoin_rows(None, None)

        self.assertEqual(context.exception.status_code, 400)

    def test_splits_normalizes_and_deduplicates_multiple_evm_wallets(self):
        other_wallet = "0x94CE9ae15c739552EeBB8A8746C0CA33c3d369Ce"

        wallets = _split_wallets(
            f"{EVM_WALLET},{other_wallet};{EVM_WALLET.upper().replace('0X', '0x')}",
            _normalize_evm_address,
            "EVM",
        )

        self.assertEqual(wallets, [EVM_WALLET, other_wallet.lower()])

    async def test_fetches_multiple_wallets_in_one_table(self):
        other_wallet = "0x94ce9ae15c739552eebb8a8746c0ca33c3d369ce"
        first_rows = [{"balance_id": "first"}]
        second_rows = [{"balance_id": "second"}]
        with patch(
            "routers.stablecoins._fetch_evm_balances",
            AsyncMock(side_effect=[first_rows, second_rows]),
        ) as fetch:
            rows = await _fetch_stablecoin_rows(
                f"{EVM_WALLET},{other_wallet}", None
            )

        self.assertEqual(rows, [*first_rows, *second_rows])
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual(fetch.await_args_list[0].args[1:], (EVM_WALLET, 1))
        self.assertEqual(fetch.await_args_list[1].args[1:], (other_wallet, 1))

    async def test_limits_total_wallets_per_request(self):
        wallets = ",".join(f"0x{index:040x}" for index in range(1, 22))

        with self.assertRaises(HTTPException) as context:
            await _fetch_stablecoin_rows(wallets, None)

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Maximum", context.exception.detail)

    async def test_reads_tron_balances_in_one_request(self):
        response = httpx.Response(
            200,
            json={
                "data": [
                    {
                        "trc20": [
                            {TRON_TOKENS[0]["address"]: "12500000"},
                            {TRON_TOKENS[1]["address"]: "3000000"},
                        ]
                    }
                ]
            },
            request=httpx.Request("GET", "https://api.trongrid.io"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = response

        rows = await _fetch_tron_balances(
            client, "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
        )

        client.get.assert_awaited_once()
        self.assertEqual(len(rows), len(TRON_TOKENS))
        self.assertEqual(rows[0]["balance"], "12.5")
        self.assertEqual(rows[1]["balance"], "3")
        self.assertEqual(rows[2]["balance"], "0")
        self.assertEqual(rows[0]["network"], "TRON")

    async def test_fetches_both_networks_and_keeps_zero_rows(self):
        ethereum_rows = [{"balance_id": "ethereum-usdc", "balance": "0"}]
        solana_rows = [{"balance_id": "solana-usdt", "balance": "0"}]
        with (
            patch(
                "routers.stablecoins._fetch_evm_balances",
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
            response = await get_stablecoin_balances_csv(
                EVM_WALLET, None, 1, None
            )

        fetch.assert_awaited_once_with(EVM_WALLET, None, 1, None)
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
