import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from routers.compound import (
    COMPOUND_CHAINS,
    _apy,
    _call_data,
    _fetch_compound_rows,
    _fetch_market_rows,
    _format,
    _market_risk_metrics,
    get_compound_positions_csv,
)


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"


class CompoundHelpersTest(unittest.TestCase):
    def test_encodes_contract_calls(self):
        data = _call_data("balanceOf(address)", ["address"], [WALLET])
        self.assertTrue(data.startswith("0x70a08231"))
        self.assertEqual(len(data), 2 + 8 + 64)

    def test_converts_per_second_rate_to_percent_apy(self):
        self.assertEqual(_format(_apy(1_000_000_000)), "3.1536")

    def test_calculates_market_ltv_and_weighted_risk_capacity(self):
        metrics = _market_risk_metrics(
            borrow_usd=50,
            collateral=[
                (100, 800_000_000_000_000_000, 850_000_000_000_000_000),
                (200, 750_000_000_000_000_000, 820_000_000_000_000_000),
            ],
        )

        self.assertEqual(
            metrics,
            {
                "ltv_percent": "16.66666666666666666666666667",
                "borrow_capacity_usd": "230",
                "liquidation_capacity_usd": "249",
                "liquidation_usage_percent": "20.08032128514056224899598394",
                "health_factor": "4.98",
            },
        )

    def test_risk_metrics_handle_no_debt_or_collateral(self):
        no_debt = _market_risk_metrics(
            borrow_usd=0,
            collateral=[
                (100, 800_000_000_000_000_000, 850_000_000_000_000_000),
            ],
        )
        self.assertEqual(no_debt["ltv_percent"], "0")
        self.assertEqual(no_debt["liquidation_usage_percent"], "0")
        self.assertEqual(no_debt["health_factor"], "")

        no_collateral = _market_risk_metrics(borrow_usd=0, collateral=[])
        self.assertEqual(no_collateral["ltv_percent"], "")
        self.assertEqual(no_collateral["borrow_capacity_usd"], "0")
        self.assertEqual(no_collateral["liquidation_capacity_usd"], "0")
        self.assertEqual(no_collateral["liquidation_usage_percent"], "")


class CompoundFetchTest(unittest.IsolatedAsyncioTestCase):
    async def test_adds_weighted_risk_metrics_to_every_market_row(self):
        chain = COMPOUND_CHAINS[1]
        market = chain.markets[0]
        base_token = "0x1111111111111111111111111111111111111111"
        base_feed = "0x2222222222222222222222222222222222222222"
        first_asset = "0x3333333333333333333333333333333333333333"
        first_feed = "0x4444444444444444444444444444444444444444"
        second_asset = "0x5555555555555555555555555555555555555555"
        second_feed = "0x6666666666666666666666666666666666666666"
        first_batch = [
            (base_token,),
            (10**6,),
            (10**8,),
            (base_feed,),
            (2,),
            (0,),
            (50 * 10**6,),
            (500_000_000_000_000_000,),
            (False,),
        ]
        asset_batch = [
            (
                0,
                first_asset,
                first_feed,
                10**18,
                800_000_000_000_000_000,
                850_000_000_000_000_000,
                900_000_000_000_000_000,
                1_000 * 10**18,
            ),
            (
                1,
                second_asset,
                second_feed,
                10**18,
                750_000_000_000_000_000,
                820_000_000_000_000_000,
                900_000_000_000_000_000,
                1_000 * 10**18,
            ),
        ]
        second_batch = [
            (10**8,),
            (0,),
            (0,),
            (1 * 10**18,),
            (100 * 10**8,),
            (2 * 10**18,),
            (100 * 10**8,),
        ]

        with patch(
            "routers.compound._rpc_batch",
            AsyncMock(side_effect=[first_batch, asset_batch, second_batch]),
        ):
            rows = await _fetch_market_rows(
                object(),
                "https://rpc.example",
                WALLET,
                1,
                chain,
                market,
            )

        self.assertEqual(len(rows), 3)
        for row in rows:
            self.assertEqual(row["ltv_percent"], "16.66666666666666666666666667")
            self.assertEqual(row["borrow_capacity_usd"], "230")
            self.assertEqual(row["liquidation_capacity_usd"], "249")
            self.assertEqual(
                row["liquidation_usage_percent"],
                "20.08032128514056224899598394",
            )
            self.assertEqual(row["health_factor"], "4.98")

    async def test_rejects_unsupported_chain(self):
        with self.assertRaises(HTTPException) as context:
            await _fetch_compound_rows(object(), WALLET, 999999)

        self.assertEqual(context.exception.status_code, 400)

    async def test_fetches_every_registered_market(self):
        rows = [{"position_id": "one"}]
        with patch(
            "routers.compound._fetch_market_rows",
            AsyncMock(return_value=rows),
        ) as fetch:
            result = await _fetch_compound_rows(object(), WALLET, 8453)

        self.assertEqual(fetch.await_count, len(COMPOUND_CHAINS[8453].markets))
        self.assertEqual(len(result), len(COMPOUND_CHAINS[8453].markets))

    async def test_route_returns_csv(self):
        row = {
            "wallet": WALLET,
            "chain_id": "1",
            "chain": "Ethereum",
            "protocol": "Compound III",
            "market": "USDC",
            "market_address": "0xmarket",
            "position_id": "0xmarket:base",
            "position_type": "base",
            "token_address": "0xtoken",
            "token_symbol": "USDC",
            "supply_amount": "100",
        }
        with patch(
            "routers.compound._fetch_compound_rows",
            AsyncMock(return_value=[row]),
        ):
            response = await get_compound_positions_csv(WALLET, 1)

        parsed = list(csv.DictReader(io.StringIO(response.body.decode())))
        self.assertEqual(parsed[0]["supply_amount"], "100")
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")


if __name__ == "__main__":
    unittest.main()
