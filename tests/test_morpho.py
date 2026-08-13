import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.morpho import (
    _fetch_morpho_rows,
    _parse_rows,
    get_morpho_positions_csv,
)


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"
MARKET = "0x" + "12" * 32
VAULT = "0x" + "34" * 20
USDC = "0x" + "56" * 20
WETH = "0x" + "78" * 20


def _payload():
    return {
        "userByAddress": {
            "marketPositions": [
                {
                    "market": {
                        "marketId": MARKET,
                        "loanAsset": {
                            "address": USDC,
                            "symbol": "USDC",
                            "name": "USD Coin",
                            "decimals": 6,
                        },
                        "collateralAsset": {
                            "address": WETH,
                            "symbol": "WETH",
                            "name": "Wrapped Ether",
                            "decimals": 18,
                        },
                        "state": {"supplyApy": "0.025", "borrowApy": "0.04"},
                    },
                    "state": {
                        "supplyAssets": "100000000",
                        "supplyAssetsUsd": "100",
                        "borrowAssets": "25000000",
                        "borrowAssetsUsd": "25",
                        "collateral": "100000000000000000",
                        "collateralUsd": "300",
                    },
                },
                {
                    "market": {"marketId": "0x" + "90" * 32},
                    "state": {
                        "supplyAssets": 0,
                        "borrowAssets": 0,
                        "collateral": 0,
                    },
                },
            ],
            "vaultPositions": [
                {
                    "vault": {
                        "address": VAULT,
                        "name": "USDC Vault",
                        "symbol": "mvUSDC",
                        "asset": {
                            "address": USDC,
                            "symbol": "USDC",
                            "name": "USD Coin",
                            "decimals": 6,
                        },
                        "state": {"netApy": "0.031"},
                    },
                    "state": {
                        "assets": "500000000",
                        "assetsUsd": "500",
                        "shares": "490",
                    },
                }
            ],
            "vaultV2Positions": [],
        }
    }


class MorphoParserTest(unittest.TestCase):
    def test_parses_active_market_and_vault_positions(self):
        rows = _parse_rows(WALLET, 1, _payload())

        self.assertEqual(len(rows), 2)
        market = next(row for row in rows if row["position_type"] == "market")
        self.assertEqual(market["position_id"], f"market:1:{MARKET}")
        self.assertEqual(market["supply_apy_percent"], "2.5")
        self.assertEqual(market["borrow_apy_percent"], "4")
        self.assertEqual(market["supply_amount"], "100")
        self.assertEqual(market["borrow_amount"], "25")
        self.assertEqual(market["collateral_amount"], "0.1")
        self.assertEqual(market["net_usd"], "375")
        vault = next(row for row in rows if row["position_type"] == "vault_v1")
        self.assertEqual(vault["supply_amount"], "500")
        self.assertEqual(vault["supply_usd"], "500")
        self.assertEqual(vault["supply_apy_percent"], "3.1")

    def test_normalizes_18_decimal_vault_assets(self):
        payload = _payload()
        position = payload["userByAddress"]["vaultPositions"][0]
        position["vault"]["asset"]["decimals"] = 18
        position["state"]["assets"] = "13002885520339213774921"

        rows = _parse_rows(WALLET, 1, payload)
        vault = next(row for row in rows if row["position_type"] == "vault_v1")

        self.assertEqual(vault["supply_amount"], "13002.885520339213774921")


class MorphoFetchTest(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_graphql_positions(self):
        response = httpx.Response(
            200,
            json={"data": _payload()},
            request=httpx.Request("POST", "https://api.morpho.org/graphql"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post.return_value = response

        rows = await _fetch_morpho_rows(client, WALLET, 1)

        self.assertEqual(len(rows), 2)
        variables = client.post.await_args.kwargs["json"]["variables"]
        self.assertEqual(variables, {"address": WALLET, "chainId": 1})

    async def test_reports_graphql_error(self):
        response = httpx.Response(
            200,
            json={"errors": [{"message": "bad query"}]},
            request=httpx.Request("POST", "https://api.morpho.org/graphql"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post.return_value = response

        with self.assertRaises(HTTPException) as context:
            await _fetch_morpho_rows(client, WALLET, 1)

        self.assertEqual(context.exception.status_code, 502)
        self.assertIn("bad query", context.exception.detail)

    async def test_route_returns_csv(self):
        rows = _parse_rows(WALLET, 1, _payload())
        with patch(
            "routers.morpho._fetch_morpho_rows",
            AsyncMock(return_value=rows),
        ):
            response = await get_morpho_positions_csv(WALLET, 1)

        parsed = list(csv.DictReader(io.StringIO(response.body.decode())))
        self.assertEqual(len(parsed), 2)
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")


if __name__ == "__main__":
    unittest.main()
