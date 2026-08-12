import csv
import base64
import io
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from routers.aave import (
    AAVE_V4_API_URL,
    _fetch_aave_rows,
    _fetch_aave_v3_rows,
    _fetch_aave_v4_onchain_rows,
    _fetch_aave_v4_rows,
    _graphql_error,
    _parse_positions,
    _parse_v4_positions,
    _render_csv,
    get_aave_positions_csv,
)


WALLET = "0xb0bc021daba3f2d737bb529c7eea2a783ae5208b"
MARKET = "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2"
TOKEN = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
V4_SPOKE = "0x1111111111111111111111111111111111111111"
V4_POSITION = "1:0x1111111111111111111111111111111111111111:42"
WSTETH = "0x7f39c581f595b53c5cb5bb9544f0728fdc16c7e0"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"


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

    def test_parses_aave_v4_main_position_and_summary(self):
        position = {
            "id": V4_POSITION,
            "spoke": {
                "id": "spoke-main",
                "name": "Main",
                "address": V4_SPOKE,
                "chain": {
                    "name": "Ethereum",
                    "chainId": 1,
                    "nativeWrappedToken": WETH,
                    "nativeInfo": {"name": "Ether", "symbol": "ETH"},
                },
            },
            "totalSupplied": {"current": {"value": "124760"}},
            "totalCollateral": {"current": {"value": "102760"}},
            "totalDebt": {"current": {"value": "0"}},
            "netBalance": {"current": {"value": "124760"}},
            "netApy": {"normalized": "1.21"},
            "netSupplyApy": {"current": {"normalized": "1.21"}},
            "netBorrowApy": {"current": {"normalized": "0"}},
            "netAccruedInterest": {"value": "481.95"},
            "healthFactor": {"current": "99"},
            "averageCollateralFactor": {"normalized": "82.1"},
        }
        rows = _parse_v4_positions(
            WALLET,
            1,
            {
                "positions": [position],
                "supplies": [
                    {
                        "id": "supply-wsteth",
                        "isCollateral": True,
                        "principal": {"amount": {"value": "11.05"}},
                        "interest": {"amount": {"value": "0"}},
                        "balance": {
                            "amount": {"value": "11.05"},
                            "exchange": {"value": "26290"},
                            "exchangeRate": {"value": "2379.1855"},
                            "isWrappedNative": False,
                            "token": {
                                "address": WSTETH,
                                "info": {
                                    "name": "Wrapped stETH",
                                    "symbol": "wstETH",
                                },
                            },
                        },
                        "reserve": {
                            "canUseAsCollateral": True,
                            "summary": {"supplyApy": {"normalized": "0"}},
                            "spoke": {"id": "spoke-main"},
                        },
                    },
                    {
                        "id": "supply-eth",
                        "isCollateral": True,
                        "principal": {"amount": {"value": "51.13"}},
                        "interest": {"amount": {"value": "0.25"}},
                        "balance": {
                            "amount": {"value": "51.38"},
                            "exchange": {"value": "98470"},
                            "exchangeRate": {"value": "1916.5045"},
                            "isWrappedNative": True,
                            "token": {
                                "address": WETH,
                                "info": {"name": "Wrapped Ether", "symbol": "WETH"},
                            },
                        },
                        "reserve": {
                            "canUseAsCollateral": True,
                            "summary": {"supplyApy": {"normalized": "1.53"}},
                            "spoke": {"id": "spoke-main"},
                        },
                    },
                ],
                "borrows": [],
            },
        )

        self.assertEqual(len(rows), 2)
        eth = next(row for row in rows if row["token_symbol"] == "ETH")
        self.assertEqual(eth["supply_amount"], "51.38")
        self.assertEqual(eth["supply_interest_amount"], "0.25")
        self.assertEqual(eth["supply_apy_percent"], "1.53")
        self.assertEqual(eth["market"], "Aave V4 Main")
        self.assertEqual(eth["protocol_version"], "v4")
        self.assertEqual(eth["position_total_supplied_usd"], "124760")
        self.assertEqual(eth["position_total_collateral_usd"], "102760")
        self.assertEqual(eth["position_net_accrued_interest_usd"], "481.95")
        self.assertEqual(eth["position_id"], f"v4:{V4_POSITION}:{WETH}")


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
            rows = await _fetch_aave_v3_rows(client, WALLET, 1)

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
                await _fetch_aave_v3_rows(object(), WALLET, 999999)

        self.assertEqual(context.exception.status_code, 400)

    async def test_fetches_all_v4_position_data_in_one_graphql_request(self):
        with patch(
            "routers.aave._fetch_graphql",
            AsyncMock(return_value={"positions": [], "supplies": [], "borrows": []}),
        ) as fetch:
            rows = await _fetch_aave_v4_rows(object(), WALLET, 1)

        self.assertEqual(rows, [])
        self.assertEqual(fetch.await_count, 1)
        self.assertEqual(fetch.await_args.args[3], AAVE_V4_API_URL)
        variables = fetch.await_args.args[2]
        self.assertEqual(
            variables["positionsRequest"],
            {"user": WALLET, "filter": {"chainIds": [1]}},
        )
        self.assertEqual(
            variables["suppliesRequest"]["query"]["userChains"],
            {"user": WALLET, "chainIds": [1]},
        )

    async def test_falls_back_to_onchain_v4_when_graphql_fails(self):
        fallback_rows = [{"position_id": "v4-onchain"}]
        client = object()
        with (
            patch(
                "routers.aave._fetch_graphql",
                AsyncMock(side_effect=HTTPException(status_code=502, detail="broken")),
            ),
            patch(
                "routers.aave._fetch_aave_v4_onchain_rows",
                AsyncMock(return_value=fallback_rows),
            ) as fallback,
        ):
            rows = await _fetch_aave_v4_rows(client, WALLET, 1)

        self.assertEqual(rows, fallback_rows)
        fallback.assert_awaited_once_with(client, WALLET, 1)

    async def test_keeps_graphql_error_when_onchain_fallback_is_empty(self):
        error = HTTPException(status_code=502, detail="broken")
        with (
            patch("routers.aave._fetch_graphql", AsyncMock(side_effect=error)),
            patch(
                "routers.aave._fetch_aave_v4_onchain_rows",
                AsyncMock(return_value=[]),
            ),
        ):
            with self.assertRaises(HTTPException) as context:
                await _fetch_aave_v4_rows(object(), WALLET, 1)

        self.assertIs(context.exception, error)

    async def test_parses_onchain_v4_main_position(self):
        reserve_count = (3,)
        reserves_and_positions = [
            (WETH, "0x2222222222222222222222222222222222222222", 0, 18, 0, 12, 0),
            (51_385_420_964_234_388_860,),
            (0,),
            (True, False),
            (0, 0, 0, 1, 0),
            (WSTETH, "0x2222222222222222222222222222222222222222", 1, 18, 0, 8, 0),
            (11_049_621_068_669_180_631,),
            (0,),
            (True, False),
            (0, 0, 0, 1, 0),
            ("0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f", "0x2222222222222222222222222222222222222222", 6, 18, 0, 12, 0),
            (0,),
            (60_802_612_732_225_340_289_900,),
            (False, True),
            (0, 0, 0, 0, 0),
        ]
        details = [
            (1_916_50000000,),
            (8_300, 0, 0),
            (2_379_18000000,),
            (8_000, 0, 0),
            (1_00000000,),
            (0, 0, 0),
        ]
        metadata = {
            WETH: ("Wrapped Ether", "WETH"),
            WSTETH: ("Wrapped stETH", "wstETH"),
            "0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f": ("GHO", "GHO"),
        }
        account_data = (
            0,
            81_0000000000000000,
            1_350000000000000000,
            10_200_000 * 10**26,
            60_802 * 10**26 * 10**27,
            2,
            1,
        )
        batch = AsyncMock(side_effect=[reserves_and_positions, details])
        call = AsyncMock(
            side_effect=[
                reserve_count,
                ("0x3333333333333333333333333333333333333333",),
                account_data,
            ]
        )
        with (
            patch("routers.aave._aave_v4_rpc_batch", batch),
            patch("routers.aave._aave_v4_rpc_call", call),
            patch("routers.aave._erc20_metadata", AsyncMock(return_value=metadata)),
        ):
            rows = await _fetch_aave_v4_onchain_rows(object(), WALLET, 1)

        self.assertEqual(len(rows), 3)
        eth = next(row for row in rows if row["token_symbol"] == "ETH")
        self.assertEqual(eth["supply_amount"], "51.38542096423438886")
        self.assertEqual(eth["token_price_usd"], "1916.5")
        self.assertEqual(eth["health_factor"], "1.35")
        self.assertEqual(eth["market"], "Aave V4 Main")
        decoded_position_id = base64.b64decode(eth["account_position_id"]).decode()
        self.assertEqual(
            decoded_position_id,
            "1::0x94e7A5dCbE816e498b89aB752661904E2F56c485::"
            "0xb0BC021DABA3f2d737bb529c7Eea2a783aE5208b",
        )
        gho = next(row for row in rows if row["token_symbol"] == "GHO")
        self.assertEqual(gho["borrow_amount"], "60802.6127322253402899")
        self.assertEqual(gho["supply_amount"], "")

    async def test_combines_v3_and_v4_rows(self):
        v3_row = {"position_id": "v3-row"}
        v4_row = {"position_id": "v4-row"}
        with (
            patch(
                "routers.aave._fetch_aave_v3_rows",
                AsyncMock(return_value=[v3_row]),
            ),
            patch(
                "routers.aave._fetch_aave_v4_rows",
                AsyncMock(return_value=[v4_row]),
            ),
        ):
            rows = await _fetch_aave_rows(object(), WALLET, 1)

        self.assertEqual(rows, [v3_row, v4_row])


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
