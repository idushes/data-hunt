import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from routers.fluid import (
    _parse_lending_positions,
    _parse_smart_lending_positions,
    _parse_vault_positions,
    _render_csv,
    get_fluid_positions_csv,
)


WALLET = "0xb0bc021daba3f2d737bb529c7eea2a783ae5208b"


class FluidPositionParserTest(unittest.TestCase):
    def test_parses_lending_position_and_ignores_empty_market(self):
        rows = _parse_lending_positions(
            WALLET,
            1,
            {
                "data": [
                    {
                        "token": {
                            "address": "0xfToken",
                            "symbol": "fUSDC",
                            "asset": {
                                "symbol": "USDC",
                                "decimals": 6,
                                "price": "1.001",
                            },
                        },
                        "totalUnderlyingAssets": "1250000",
                    },
                    {
                        "token": {
                            "address": "0xempty",
                            "symbol": "fWETH",
                            "asset": {
                                "symbol": "WETH",
                                "decimals": 18,
                                "price": "2000",
                            },
                        },
                        "totalUnderlyingAssets": "0",
                    },
                ]
            },
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["supply_amount_1"], "1.25")
        self.assertEqual(rows[0]["net_usd"], "1.25125")

    def test_parses_vault_position(self):
        rows = _parse_vault_positions(
            WALLET,
            1,
            [
                {
                    "id": "1",
                    "supply": "1000000000000000000",
                    "borrow": "500000000",
                    "isLiquidated": False,
                    "vault": {
                        "id": "1",
                        "address": "0xvault",
                        "liquidationThreshold": 9000,
                        "supplyToken": {
                            "token0": {
                                "address": "0xeth",
                                "symbol": "ETH",
                                "decimals": 18,
                                "price": "2000",
                            },
                            "token1": {
                                "address": "0x0000000000000000000000000000000000000000"
                            },
                        },
                        "borrowToken": {
                            "token0": {
                                "address": "0xusdc",
                                "symbol": "USDC",
                                "decimals": 6,
                                "price": "1",
                            },
                            "token1": {
                                "address": "0x0000000000000000000000000000000000000000"
                            },
                        },
                    },
                }
            ],
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market"], "ETH/USDC")
        self.assertEqual(rows[0]["supply_amount_1"], "1")
        self.assertEqual(rows[0]["borrow_amount_1"], "500")
        self.assertEqual(rows[0]["net_usd"], "1500")
        self.assertEqual(rows[0]["ltv_percent"], "25")
        self.assertEqual(rows[0]["liquidation_threshold_percent"], "90")

    def test_parses_smart_lending_position(self):
        rows = _parse_smart_lending_positions(
            WALLET,
            1,
            {
                "data": [
                    {
                        "token": {
                            "address": "0xfsl",
                            "symbol": "fSL",
                            "tokens": {
                                "token0": {
                                    "address": "0xa",
                                    "symbol": "USDC",
                                    "decimals": 6,
                                    "price": "1",
                                },
                                "token1": {
                                    "address": "0xb",
                                    "symbol": "USDT",
                                    "decimals": 6,
                                    "price": "1",
                                },
                            },
                        },
                        "underlyingAssetsToken0": "1200000",
                        "underlyingAssetsToken1": "800000",
                    }
                ]
            },
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market"], "USDC-USDT")
        self.assertEqual(rows[0]["supply_amount_1"], "1.2")
        self.assertEqual(rows[0]["supply_amount_2"], "0.8")
        self.assertEqual(rows[0]["net_usd"], "2")

    def test_csv_has_stable_header_when_there_are_no_positions(self):
        parsed = list(csv.reader(io.StringIO(_render_csv([]))))
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0][0:4], ["wallet", "chain_id", "chain", "position_type"])


class FluidPositionRouteTest(unittest.IsolatedAsyncioTestCase):
    async def test_returns_csv_rows_from_fetcher(self):
        rows = [
            {
                "wallet": WALLET,
                "chain_id": "1",
                "chain": "ethereum",
                "position_type": "lending",
                "position_id": "0xfToken",
            }
        ]

        with (
            patch("routers.fluid._get_cached_csv", return_value=None),
            patch("routers.fluid._set_cached_csv"),
            patch(
                "routers.fluid._fetch_fluid_rows", AsyncMock(return_value=rows)
            ),
        ):
            response = await get_fluid_positions_csv(WALLET, 1)

        self.assertEqual(response.status_code, 200)
        self.assertIn("lending", response.body.decode())

    async def test_rejects_unsupported_chain(self):
        with self.assertRaises(HTTPException) as context:
            await get_fluid_positions_csv(WALLET, 10)

        self.assertEqual(context.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
