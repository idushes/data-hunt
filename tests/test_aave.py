import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from routers.aave import (
    _fetch_aave_rows,
    _graphql_error,
    _parse_positions,
    _render_csv,
    get_aave_positions_csv,
)


WALLET = "0xb0bc021daba3f2d737bb529c7eea2a783ae5208b"
MARKET = "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2"
TOKEN = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


def _market():
    return {
        "name": "Aave V3 Ethereum",
        "address": MARKET,
        "chain": {"name": "Ethereum", "chainId": 1},
        "userState": {
            "healthFactor": "1.75",
            "ltv": {"formatted": "52.4"},
            "currentLiquidationThreshold": {"formatted": "80.5"},
        },
    }


def _currency():
    return {
        "address": TOKEN,
        "name": "USD Coin",
        "symbol": "USDC",
        "chainId": 1,
    }


class AavePositionParserTest(unittest.TestCase):
    def test_combines_supply_and_borrow_for_same_market_reserve(self):
        market = _market()
        currency = _currency()
        rows = _parse_positions(
            WALLET,
            1,
            [market],
            {
                "supplies": [
                    {
                        "market": market,
                        "currency": currency,
                        "balance": {
                            "usdPerToken": "1",
                            "usd": "125.25",
                            "amount": {"value": "125.25"},
                        },
                        "apy": {"formatted": "3.14"},
                        "isCollateral": True,
                        "canBeCollateral": True,
                    }
                ],
                "borrows": [
                    {
                        "market": market,
                        "currency": currency,
                        "debt": {
                            "usdPerToken": "1",
                            "usd": "25.2",
                            "amount": {"value": "25.2"},
                        },
                        "apy": {"formatted": "5.01"},
                    }
                ],
            },
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["position_id"], f"{MARKET}:{TOKEN}")
        self.assertEqual(rows[0]["supply_amount"], "125.25")
        self.assertEqual(rows[0]["borrow_amount"], "25.2")
        self.assertEqual(rows[0]["net_usd"], "100.05")
        self.assertEqual(rows[0]["health_factor"], "1.75")
        self.assertEqual(rows[0]["is_collateral"], "true")

    def test_keeps_borrow_only_position_and_stable_key(self):
        market = _market()
        currency = _currency()
        rows = _parse_positions(
            WALLET,
            1,
            [market],
            {
                "supplies": [],
                "borrows": [
                    {
                        "market": market,
                        "currency": currency,
                        "debt": {
                            "usdPerToken": "1",
                            "usd": "12.5",
                            "amount": {"value": "12.5"},
                        },
                        "apy": {"formatted": "4.2"},
                    }
                ],
            },
        )

        self.assertEqual(rows[0]["position_id"], f"{MARKET}:{TOKEN}")
        self.assertEqual(rows[0]["supply_usd"], "")
        self.assertEqual(rows[0]["net_usd"], "-12.5")

    def test_csv_has_stable_header_when_there_are_no_positions(self):
        parsed = list(csv.reader(io.StringIO(_render_csv([]))))
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0][0:4], ["wallet", "chain_id", "chain", "market"])

    def test_reports_first_graphql_error(self):
        self.assertEqual(
            _graphql_error({"errors": [{"message": "bad request"}]}),
            "Aave API error: bad request",
        )


class AaveFetchTest(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_markets_before_positions(self):
        client = object()
        market = _market()
        currency = _currency()
        position_data = {
            "supplies": [
                {
                    "market": market,
                    "currency": currency,
                    "balance": {
                        "usdPerToken": "1",
                        "usd": "10",
                        "amount": {"value": "10"},
                    },
                    "apy": {"formatted": "2"},
                    "isCollateral": True,
                    "canBeCollateral": True,
                }
            ],
            "borrows": [],
        }

        with patch(
            "routers.aave._fetch_graphql",
            AsyncMock(side_effect=[{"value": [market]}, position_data]),
        ) as fetch:
            rows = await _fetch_aave_rows(client, WALLET, 1)

        self.assertEqual(len(rows), 1)
        self.assertEqual(fetch.await_count, 2)
        position_variables = fetch.await_args_list[1].args[2]
        self.assertEqual(
            position_variables["suppliesRequest"]["markets"],
            [{"address": MARKET, "chainId": 1}],
        )

    async def test_rejects_chain_without_aave_markets(self):
        with patch(
            "routers.aave._fetch_graphql",
            AsyncMock(return_value={"value": []}),
        ):
            with self.assertRaises(HTTPException) as context:
                await _fetch_aave_rows(object(), WALLET, 999999)

        self.assertEqual(context.exception.status_code, 400)


class AavePositionRouteTest(unittest.IsolatedAsyncioTestCase):
    async def test_returns_csv_rows_from_fetcher(self):
        rows = [
            {
                "wallet": WALLET,
                "chain_id": "1",
                "chain": "Ethereum",
                "market": "Aave V3 Ethereum",
                "market_address": MARKET,
                "position_id": f"{MARKET}:{TOKEN}",
                "token_symbol": "USDC",
            }
        ]

        with patch(
            "routers.aave._fetch_aave_rows",
            AsyncMock(return_value=rows),
        ):
            response = await get_aave_positions_csv(WALLET, 1)

        self.assertEqual(response.status_code, 200)
        self.assertIn("USDC", response.body.decode())
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")

    async def test_rejects_invalid_address(self):
        with self.assertRaises(HTTPException) as context:
            await get_aave_positions_csv("not-an-address", 1)

        self.assertEqual(context.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
