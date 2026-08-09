import csv
import io
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.stakedao import (
    _build_rows,
    _fetch_balances,
    _fetch_stakedao_rows,
    _fetch_v2_vault_states,
    _locker_targets,
    _render_csv,
    _strategy_targets,
    _v2_vault_candidates,
    _v2_vault_targets,
    get_stakedao_positions_csv,
)


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"
GAUGE = "0x1111111111111111111111111111111111111111"
VAULT = "0x2222222222222222222222222222222222222222"
TOKEN = "0x3333333333333333333333333333333333333333"
V2_VAULT = "0xe70fc8ffb97f6d539e5d50f657f4aae69f01b87d"
V2_LP = "0x47ab5f9d8c9c7d002a92320f23a696d348c56a7f"


def _strategy_payload():
    return {
        "deployed": [
            {
                "key": "pool-1",
                "name": "USDC/crvUSD",
                "protocol": "curve",
                "chainId": 1,
                "vault": VAULT,
                "lpToken": {
                    "symbol": "USDC-crvUSD",
                    "address": TOKEN,
                    "decimals": 18,
                },
                "sdGauge": {"address": GAUGE},
                "lpPriceInUsd": 1.25,
                "tvl": 5000,
                "apr": {
                    "current": {"total": 4.5},
                    "projected": {"total": 5.25},
                },
                "minApr": 3,
                "maxApr": 7,
            }
        ]
    }


class StakeDaoCatalogTest(unittest.TestCase):
    def test_strategy_creates_staked_and_vault_targets(self):
        targets = _strategy_targets(_strategy_payload(), "curve", 1)

        self.assertEqual(len(targets), 2)
        self.assertEqual(
            {target["position_type"] for target in targets}, {"staked", "vault"}
        )
        self.assertEqual(targets[0]["asset_symbol"], "USDC-crvUSD")
        self.assertEqual(targets[0]["apr_projected_percent"], 5.25)

    def test_locker_creates_staked_wallet_and_autocompounder_targets(self):
        targets = _locker_targets(
            {
                "parsed": [
                    {
                        "id": "crv",
                        "protocol": "Curve",
                        "chainId": 1,
                        "sdToken": {
                            "symbol": "sdCRV",
                            "address": TOKEN,
                            "decimals": 18,
                        },
                        "modules": {"gauge": GAUGE},
                        "autoCompounder": {"aSdToken": VAULT},
                        "sdTokenPriceInUsd": 0.5,
                    }
                ]
            },
            1,
        )

        self.assertEqual(len(targets), 3)
        self.assertEqual(
            {target["position_type"] for target in targets},
            {"staked", "wallet", "autocompounder"},
        )
        auto = next(
            target for target in targets if target["position_type"] == "autocompounder"
        )
        self.assertIsNone(auto["price_usd"])

    def test_builds_stable_position_row_and_value(self):
        target = _strategy_targets(_strategy_payload(), "curve", 1)[0]

        rows = _build_rows(WALLET, 1, "Ethereum", [target], [2 * 10**18])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["amount"], "2")
        self.assertEqual(rows[0]["value_usd"], "2.5")
        self.assertEqual(
            rows[0]["position_id"], f"1:staked:{GAUGE}:pool-1"
        )

    def test_omits_zero_balance_targets(self):
        target = _strategy_targets(_strategy_payload(), "curve", 1)[0]
        self.assertEqual(_build_rows(WALLET, 1, "Ethereum", [target], [0]), [])

    def test_discovers_stake_dao_v2_vault_token(self):
        candidates = _v2_vault_candidates(
            [
                {
                    "token": {
                        "address_hash": V2_VAULT,
                        "name": "Stake DAO frxUSD/sUSDe Vault",
                        "symbol": "sd-frxsUSDe-vault",
                        "decimals": "18",
                    },
                    "value": "20752878510179439248607",
                },
                {
                    "token": {
                        "address_hash": TOKEN,
                        "name": "Fake Vault",
                        "symbol": "vault",
                    },
                    "value": "1",
                },
            ]
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["position_contract"], V2_VAULT)
        self.assertEqual(candidates[0]["name"], "frxUSD/sUSDe")

    def test_enriches_v2_vault_with_curve_pool_price_and_tvl(self):
        targets, balances = _v2_vault_targets(
            [
                {
                    "position_contract": V2_VAULT,
                    "name": "frxUSD/sUSDe",
                    "asset_address": V2_LP,
                    "raw_balance": 20_752_878_510_179_439_248_607,
                    "total_assets": 453_186_687_944_980_701_453_954,
                }
            ],
            {
                "data": {
                    "poolData": [
                        {
                            "id": "factory-stable-ng-886",
                            "name": "frxUSD/sUSDe",
                            "symbol": "frxUSDsUSDe-f",
                            "address": V2_LP,
                            "lpTokenAddress": V2_LP,
                            "decimals": "18",
                            "totalSupply": "530566975771215704257179",
                            "usdTotal": "533598.7132574318",
                            "gaugeCrvApy": [7.03, 17.58],
                        }
                    ]
                }
            },
            1,
        )

        self.assertEqual(balances, [20_752_878_510_179_439_248_607])
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0]["position_type"], "vault_v2")
        self.assertEqual(targets[0]["strategy_key"], "factory-stable-ng-886")
        self.assertEqual(targets[0]["apr_min_percent"], Decimal("7.03"))
        self.assertEqual(targets[0]["apr_max_percent"], Decimal("17.58"))
        rows = _build_rows(WALLET, 1, "Ethereum", targets, balances)
        self.assertEqual(rows[0]["amount"], "20752.878510179439248607")
        self.assertEqual(rows[0]["underlying_tvl_usd"], "533598.7132574318")
        self.assertTrue(Decimal(rows[0]["value_usd"]) > Decimal("20800"))


class StakeDaoRpcTest(unittest.IsolatedAsyncioTestCase):
    async def test_reads_all_balances_in_one_rpc_batch(self):
        response = httpx.Response(
            200,
            json=[
                {"jsonrpc": "2.0", "id": 0, "result": hex(10**18)},
                {"jsonrpc": "2.0", "id": 1, "result": "0x"},
            ],
            request=httpx.Request("POST", "https://rpc.example"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post.return_value = response
        targets = _strategy_targets(_strategy_payload(), "curve", 1)

        balances = await _fetch_balances(
            client, "https://rpc.example", WALLET, targets
        )

        client.post.assert_awaited_once()
        self.assertEqual(balances, [10**18, 0])

    async def test_reads_v2_vault_asset_and_share_ratio_in_one_rpc_batch(self):
        user_shares = 20_752_878_510_179_439_248_607
        total_assets = 453_186_687_944_980_701_453_954
        total_supply = total_assets
        response = httpx.Response(
            200,
            json=[
                {"jsonrpc": "2.0", "id": 0, "result": hex(user_shares)},
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": f"0x{V2_LP[2:].rjust(64, '0')}",
                },
                {"jsonrpc": "2.0", "id": 2, "result": hex(total_assets)},
                {"jsonrpc": "2.0", "id": 3, "result": hex(total_supply)},
            ],
            request=httpx.Request("POST", "https://rpc.example"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post.return_value = response

        states = await _fetch_v2_vault_states(
            client,
            "https://rpc.example",
            WALLET,
            [{"position_contract": V2_VAULT, "name": "frxUSD/sUSDe"}],
        )

        client.post.assert_awaited_once()
        self.assertEqual(len(states), 1)
        self.assertEqual(states[0]["asset_address"], V2_LP)
        self.assertEqual(states[0]["raw_balance"], user_shares)

    async def test_rejects_unsupported_chain(self):
        with self.assertRaises(HTTPException) as context:
            await _fetch_stakedao_rows(WALLET, 999999)

        self.assertEqual(context.exception.status_code, 400)


class StakeDaoRouteTest(unittest.IsolatedAsyncioTestCase):
    async def test_route_returns_csv(self):
        row = {column: "" for column in next(csv.reader(io.StringIO(_render_csv([]))))}
        row.update(
            {
                "wallet": WALLET,
                "position_id": f"1:staked:{GAUGE}:pool-1",
                "amount": "2",
            }
        )
        with patch(
            "routers.stakedao._fetch_stakedao_rows",
            AsyncMock(return_value=[row]),
        ) as fetch:
            response = await get_stakedao_positions_csv(WALLET, 1)

        fetch.assert_awaited_once_with(WALLET, 1)
        self.assertIn("pool-1", response.body.decode())
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")


if __name__ == "__main__":
    unittest.main()
