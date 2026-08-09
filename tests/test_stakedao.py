import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.stakedao import (
    _build_rows,
    _fetch_balances,
    _fetch_stakedao_rows,
    _locker_targets,
    _render_csv,
    _strategy_targets,
    get_stakedao_positions_csv,
)


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"
GAUGE = "0x1111111111111111111111111111111111111111"
VAULT = "0x2222222222222222222222222222222222222222"
TOKEN = "0x3333333333333333333333333333333333333333"


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
