import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from routers.fluid import (
    _parse_lending_positions,
    _parse_lite_eth_position,
    _parse_lite_usd_position,
    _parse_smart_lending_positions,
    _parse_vault_positions,
    _render_csv,
    get_fluid_positions_csv,
)


WALLET = "0xb0bc021daba3f2d737bb529c7eea2a783ae5208b"
NO_STAKING: dict[str, str] = {}


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
                            "totalRate": "541",
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
            NO_STAKING,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["supply_amount_1"], "1.25")
        self.assertEqual(rows[0]["net_usd"], "1.25125")
        self.assertEqual(rows[0]["supply_apr_percent"], "5.41")
        self.assertEqual(rows[0]["net_apr_percent"], "5.41")
        self.assertEqual(rows[0]["borrow_apr_percent"], "")

    def test_lending_apr_adds_staking_yield_and_reward_emissions(self):
        rows = _parse_lending_positions(
            WALLET,
            1,
            {
                "data": [
                    {
                        "token": {
                            "address": "0xfwsteth",
                            "symbol": "fwstETH",
                            "totalRate": "3",
                            "rewards": [{"type": "supply", "rate": "50"}],
                            "asset": {
                                "address": "0xWSTETH",
                                "symbol": "wstETH",
                                "decimals": 18,
                                "price": "3000",
                            },
                        },
                        "totalUnderlyingAssets": "1000000000000000000",
                    }
                ]
            },
            {"0xwsteth": "218"},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["supply_apr_percent"], "2.71")

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
                        "supplyRate": {
                            "liquidity": {"token0": "170", "token1": "0"},
                            "dex": {"trading": "0"},
                        },
                        "borrowRate": {
                            "liquidity": {"token0": "682", "token1": "0"},
                            "dex": {"trading": "0"},
                        },
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
            NO_STAKING,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market"], "ETH/USDC")
        self.assertEqual(rows[0]["supply_amount_1"], "1")
        self.assertEqual(rows[0]["borrow_amount_1"], "500")
        self.assertEqual(rows[0]["net_usd"], "1500")
        self.assertEqual(rows[0]["ltv_percent"], "25")
        self.assertEqual(rows[0]["liquidation_threshold_percent"], "90")
        self.assertEqual(rows[0]["supply_apr_percent"], "1.7")
        self.assertEqual(rows[0]["borrow_apr_percent"], "6.82")
        # 2000 USD collateral at 1.70% against 500 USD debt at 6.82%, over 1500 USD equity.
        self.assertEqual(rows[0]["net_apr_percent"], "-0.0067")

    def test_vault_net_apr_blends_dex_pair_by_usd_share(self):
        rows = _parse_vault_positions(
            WALLET,
            1,
            [
                {
                    "id": "2",
                    "supply": "1000000000000000000",
                    "borrow": "0",
                    "vault": {
                        "id": "2",
                        "address": "0xsmartvault",
                        "supplyRate": {
                            "liquidity": {"token0": "10", "token1": "170"},
                            "dex": {"trading": "15"},
                        },
                        "rewards": [{"type": "supply", "rate": "100"}],
                        "supplyDexData": {
                            # At 3000 and 1000 per token these legs are worth the same.
                            "token0PerShare": "250000000000000000",
                            "token1PerShare": "750000000000000000",
                        },
                        "supplyToken": {
                            "token0": {
                                "address": "0xWSTETH",
                                "symbol": "wstETH",
                                "decimals": 18,
                                "price": "3000",
                            },
                            "token1": {
                                "address": "0xeth",
                                "symbol": "ETH",
                                "decimals": 18,
                                "price": "1000",
                            },
                        },
                        "borrowToken": {
                            "token0": {
                                "address": "0x0000000000000000000000000000000000000000"
                            }
                        },
                    },
                }
            ],
            {"0xwsteth": "218"},
        )

        self.assertEqual(len(rows), 1)
        # (10 + 218) / 2 + 170 / 2 blended, plus 15 bps of trading fees and 100 bps of rewards.
        self.assertEqual(rows[0]["supply_apr_percent"], "3.14")
        self.assertEqual(rows[0]["net_apr_percent"], "3.14")

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
                            "rate": {
                                "liquidity": {"token0": "0", "token1": "600"},
                                "dex": {"trading": "31"},
                            },
                            "dex": {
                                "token0PerShare": "500000000000000000",
                                "token1PerShare": "500000000000000000",
                            },
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
            NO_STAKING,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market"], "USDC-USDT")
        self.assertEqual(rows[0]["supply_amount_1"], "1.2")
        self.assertEqual(rows[0]["supply_amount_2"], "0.8")
        self.assertEqual(rows[0]["net_usd"], "2")
        # Half of the 600 bps leg, plus the pool's 31 bps of trading fees.
        self.assertEqual(rows[0]["supply_apr_percent"], "3.31")
        self.assertEqual(rows[0]["net_apr_percent"], "3.31")

    def test_parses_lite_eth_position(self):
        rows = _parse_lite_eth_position(
            WALLET,
            1,
            [
                {
                    "version": "2",
                    "vault": "0xliteeth",
                    "userSupplyAmount": "1.5",
                    "token": {"symbol": "ETH", "price": "2000"},
                    "apy": {
                        "apyWithoutFee": "4.3891357327363883424",
                        "apyWithFee": "5.486419665920485428",
                    },
                }
            ],
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["position_type"], "lite_eth")
        self.assertEqual(rows[0]["supply_amount_1"], "1.5")
        self.assertEqual(rows[0]["net_usd"], "3000")
        # Fluid Lite already reports a percentage, so it is not converted from basis points.
        self.assertEqual(rows[0]["supply_apr_percent"], "4.3891")
        self.assertEqual(rows[0]["net_apr_percent"], "4.3891")

    def test_parses_lite_usd_position(self):
        rows = _parse_lite_usd_position(
            WALLET,
            1,
            {
                "success": True,
                "data": {
                    "address": "0xliteusd",
                    "symbol": "fLiteUSD",
                    "rate": "730",
                    "underlyingAsset": {
                        "symbol": "USDC",
                        "decimals": 6,
                        "price": "0.9995",
                    },
                },
            },
            {
                "success": True,
                "data": {"assets": "29182750875"},
            },
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["position_type"], "lite_usdc")
        self.assertEqual(rows[0]["supply_amount_1"], "29182.750875")
        self.assertEqual(rows[0]["net_usd"], "29168.1594995625")
        self.assertEqual(rows[0]["supply_apr_percent"], "7.3")
        self.assertEqual(rows[0]["net_apr_percent"], "7.3")

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
