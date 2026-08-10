import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from routers.euler import _fetch_euler_rows, _parse_rows, get_euler_positions_csv


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"
ACCOUNT = "0x" + "12" * 20
VAULT = "0x" + "34" * 20
TOKEN = "0x" + "56" * 20


def _payload():
    return {
        "data": {
            "portfolio": {
                "positions": [
                    {
                        "account": ACCOUNT,
                        "vaultAddress": VAULT,
                        "vault": {
                            "address": VAULT,
                            "type": "evk",
                            "name": "USDC Vault",
                            "symbol": "eUSDC",
                            "asset": {
                                "address": TOKEN,
                                "symbol": "USDC",
                                "name": "USD Coin",
                                "decimals": 6,
                            },
                        },
                        "shares": "99000000",
                        "assets": "100000000",
                        "borrowed": "25000000",
                        "marketPriceUsd": 1,
                        "suppliedValueUsd": 100,
                        "borrowedValueUsd": 25,
                        "isCollateral": True,
                        "isController": True,
                        "liquidity": {
                            "liabilityValueUsd": 25,
                            "totalCollateralValueUsd": 75,
                            "daysToLiquidation": "MoreThanAYear",
                        },
                    }
                ]
            }
        }
    }


class EulerParserTest(unittest.TestCase):
    def test_parses_portfolio_position(self):
        rows = _parse_rows(WALLET, 1, _payload())

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["supply_amount"], "100")
        self.assertEqual(rows[0]["borrow_amount"], "25")
        self.assertEqual(rows[0]["health_factor"], "3")
        self.assertEqual(rows[0]["position_id"], f"1:{ACCOUNT}:{VAULT}")


class EulerFetchTest(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_official_portfolio_endpoint(self):
        response = httpx.Response(
            200,
            json=_payload(),
            request=httpx.Request("GET", "https://v3.euler.finance"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = response

        rows = await _fetch_euler_rows(client, WALLET, 8453)

        self.assertEqual(len(rows), 1)
        self.assertEqual(client.get.await_args.kwargs["params"], {"chainId": 8453})

    async def test_route_returns_csv(self):
        rows = _parse_rows(WALLET, 1, _payload())
        with patch(
            "routers.euler._fetch_euler_rows",
            AsyncMock(return_value=rows),
        ):
            response = await get_euler_positions_csv(WALLET, 1)

        parsed = list(csv.DictReader(io.StringIO(response.body.decode())))
        self.assertEqual(parsed[0]["vault_symbol"], "eUSDC")
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")


if __name__ == "__main__":
    unittest.main()
