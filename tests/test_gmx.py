import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.gmx import (
    _fetch_gmx_rows,
    _parse_rows,
    get_gmx_positions_csv,
)


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"
MARKET = "0x" + "12" * 20
INDEX = "0x" + "34" * 20
USDC = "0x" + "56" * 20
WETH = "0x" + "78" * 20


def _positions():
    return [
        {
            "key": f"{WALLET}:{MARKET}:{USDC}:false",
            "contractKey": "0x" + "90" * 32,
            "marketAddress": MARKET,
            "collateralTokenAddress": USDC,
            "sizeInUsd": str(7_179 * 10**30),
            "sizeInTokens": str(29_750 * 10**18),
            "collateralAmount": str(495 * 10**6),
            "remainingCollateralAmount": str(493 * 10**6),
            "collateralUsd": str(495 * 10**30),
            "remainingCollateralUsd": str(493 * 10**30),
            "entryPrice": str(24 * 10**28),
            "markPrice": str(25 * 10**28),
            "liquidationPrice": str(26 * 10**28),
            "leverage": "145417",
            "pnl": str(45 * 10**30),
            "pnlPercentage": "927",
            "pnlAfterFees": str(44 * 10**30),
            "pnlAfterFeesPercentage": "897",
            "pnlAfterAllFees": str(37 * 10**30),
            "pnlAfterAllFeesPercentage": "760",
            "netValue": str(539 * 10**30),
            "netValueAfterAllFees": str(532 * 10**30),
            "pendingBorrowingFeesUsd": str(1 * 10**30),
            "pendingFundingFeesUsd": "0",
            "pendingClaimableFundingFeesUsd": str(2 * 10**30),
            "closingFeeUsd": str(4 * 10**30),
            "positionFeeAmount": str(4 * 10**6),
            "fundingFeeAmount": "0",
            "claimableLongTokenAmount": str(10**17),
            "claimableShortTokenAmount": str(1_400_000),
            "isLong": False,
            "hasLowCollateral": False,
            "increasedAtTime": "1720000000",
            "decreasedAtTime": "0",
            "indexName": "CRV/USD",
            "poolName": "WETH-USDC",
            "relatedOrders": [{"key": "order"}],
        }
    ]


def _markets():
    return [
        {
            "symbol": "CRV/USD [WETH-USDC]",
            "marketTokenAddress": MARKET,
            "indexTokenAddress": INDEX,
            "longTokenAddress": WETH,
            "shortTokenAddress": USDC,
        }
    ]


def _tokens():
    return [
        {"address": INDEX, "symbol": "CRV", "name": "Curve", "decimals": 18},
        {"address": WETH, "symbol": "WETH", "name": "Wrapped Ether", "decimals": 18},
        {"address": USDC, "symbol": "USDC", "name": "USD Coin", "decimals": 6},
    ]


class GmxParserTest(unittest.TestCase):
    def test_normalizes_position_units(self):
        rows = _parse_rows(WALLET, 42161, _positions(), _markets(), _tokens())

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["direction"], "short")
        self.assertEqual(row["collateral_amount"], "495")
        self.assertEqual(row["size_tokens"], "29750")
        self.assertEqual(row["size_usd"], "7179")
        self.assertEqual(row["entry_price_usd"], "0.24")
        self.assertEqual(row["leverage"], "14.5417")
        self.assertEqual(row["pnl_percent"], "9.27")
        self.assertEqual(row["claimable_short_token_amount"], "1.4")
        self.assertEqual(row["related_orders_count"], "1")


class GmxFetchTest(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_positions_then_metadata(self):
        responses = [
            httpx.Response(200, json=_positions()),
            httpx.Response(200, json=_markets()),
            httpx.Response(200, json=_tokens()),
        ]
        for response in responses:
            response.request = httpx.Request("GET", "https://arbitrum.gmxapi.io")
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.side_effect = responses

        rows = await _fetch_gmx_rows(client, WALLET, 42161)

        self.assertEqual(len(rows), 1)
        self.assertEqual(client.get.await_count, 3)
        first = client.get.await_args_list[0]
        self.assertEqual(first.kwargs["params"]["address"], WALLET)

    async def test_empty_wallet_skips_metadata_requests(self):
        response = httpx.Response(
            200,
            json=[],
            request=httpx.Request("GET", "https://arbitrum.gmxapi.io"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = response

        rows = await _fetch_gmx_rows(client, WALLET, 42161)

        self.assertEqual(rows, [])
        self.assertEqual(client.get.await_count, 1)

    async def test_reports_upstream_failure(self):
        response = httpx.Response(
            429,
            request=httpx.Request("GET", "https://arbitrum.gmxapi.io"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = response

        with self.assertRaises(HTTPException) as context:
            await _fetch_gmx_rows(client, WALLET, 42161)

        self.assertEqual(context.exception.status_code, 502)

    async def test_route_returns_csv(self):
        rows = _parse_rows(WALLET, 42161, _positions(), _markets(), _tokens())
        with patch(
            "routers.gmx._fetch_gmx_rows",
            AsyncMock(return_value=rows),
        ):
            response = await get_gmx_positions_csv(WALLET, 42161)

        parsed = list(csv.DictReader(io.StringIO(response.body.decode())))
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["position_id"], rows[0]["position_id"])
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")

    async def test_rejects_unsupported_chain(self):
        with self.assertRaises(HTTPException) as context:
            await get_gmx_positions_csv(WALLET, 1)

        self.assertEqual(context.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
