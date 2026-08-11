import csv
import io
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.polymarket import (
    POLYMARKET_PAGE_SIZE,
    POLYMARKET_PUSD_ADDRESS,
    _fetch_pusd_balance,
    _fetch_polymarket_rows,
    _fetch_positions,
    _normalize_wallet,
    _parse_rows,
    get_polymarket_positions_csv,
)


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"


def _position(asset: str = "123456789") -> dict[str, object]:
    return {
        "proxyWallet": WALLET,
        "asset": asset,
        "conditionId": "0x" + "12" * 32,
        "size": 200,
        "avgPrice": 0.4,
        "initialValue": 80,
        "currentValue": 110,
        "cashPnl": 30,
        "percentPnl": 37.5,
        "totalBought": 250,
        "realizedPnl": 5,
        "percentRealizedPnl": 6.25,
        "curPrice": 0.55,
        "redeemable": True,
        "mergeable": False,
        "title": "Will this test pass?",
        "slug": "will-this-test-pass",
        "icon": "https://example.com/icon.png",
        "eventSlug": "test-event",
        "outcome": "Yes",
        "outcomeIndex": 0,
        "oppositeOutcome": "No",
        "oppositeAsset": "987654321",
        "endDate": "2026-12-31",
        "negativeRisk": False,
    }


class PolymarketParserTest(unittest.TestCase):
    def test_normalizes_wallet(self):
        self.assertEqual(_normalize_wallet(WALLET.upper().replace("0X", "0x")), WALLET)

        with self.assertRaises(HTTPException):
            _normalize_wallet("not-an-address")

    def test_returns_stable_summary_and_position_rows(self):
        rows = _parse_rows(
            WALLET,
            [_position()],
            Decimal("110"),
            Decimal("99.3341"),
        )

        self.assertEqual(len(rows), 3)
        summary, pusd, position = rows
        self.assertEqual(summary["position_id"], f"{WALLET}:portfolio")
        self.assertEqual(summary["row_type"], "portfolio_summary")
        self.assertEqual(summary["current_value_usd"], "110")
        self.assertEqual(summary["total_account_value_usd"], "209.3341")
        self.assertEqual(summary["cash_pnl_usd"], "30")
        self.assertEqual(summary["percent_pnl"], "37.5")
        self.assertEqual(pusd["position_id"], f"{WALLET}:pusd")
        self.assertEqual(pusd["row_type"], "collateral_balance")
        self.assertEqual(pusd["token_symbol"], "pUSD")
        self.assertEqual(pusd["token_address"], POLYMARKET_PUSD_ADDRESS)
        self.assertEqual(pusd["balance"], "99.3341")
        self.assertEqual(pusd["balance_usd"], "99.3341")
        self.assertEqual(position["position_id"], "123456789")
        self.assertEqual(position["current_price"], "0.55")
        self.assertEqual(position["redeemable"], "true")
        self.assertEqual(
            position["market_url"], "https://polymarket.com/event/test-event"
        )


class PolymarketFetchTest(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_pusd_balance_from_official_contract(self):
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post.return_value = httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": 1, "result": hex(99_334_100)},
            request=httpx.Request("POST", "https://polygon-bor-rpc.publicnode.com"),
        )

        balance = await _fetch_pusd_balance(client, WALLET)

        self.assertEqual(balance, Decimal("99.3341"))
        call = client.post.await_args
        self.assertEqual(
            call.kwargs["json"]["params"][0]["to"],
            POLYMARKET_PUSD_ADDRESS,
        )
        self.assertTrue(call.kwargs["json"]["params"][0]["data"].endswith(WALLET[2:]))

    async def test_paginates_positions(self):
        first_page = [_position(str(index)) for index in range(POLYMARKET_PAGE_SIZE)]
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.side_effect = [
            httpx.Response(
                200,
                json=first_page,
                request=httpx.Request("GET", "https://data-api.polymarket.com"),
            ),
            httpx.Response(
                200,
                json=[_position("last")],
                request=httpx.Request("GET", "https://data-api.polymarket.com"),
            ),
        ]

        positions = await _fetch_positions(client, WALLET, 1)

        self.assertEqual(len(positions), POLYMARKET_PAGE_SIZE + 1)
        self.assertEqual(client.get.await_count, 2)
        self.assertEqual(client.get.await_args_list[1].kwargs["params"]["offset"], 500)

    async def test_fetches_positions_and_portfolio_value(self):
        async def get(url, params):
            payload = (
                [_position()]
                if url.endswith("/positions")
                else [{"user": WALLET, "value": 110}]
            )
            return httpx.Response(
                200,
                json=payload,
                request=httpx.Request("GET", url),
            )

        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.side_effect = get
        client.post.return_value = httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": 1, "result": hex(99_334_100)},
            request=httpx.Request("POST", "https://polygon-bor-rpc.publicnode.com"),
        )

        rows = await _fetch_polymarket_rows(client, WALLET, 1)

        self.assertEqual(rows[0]["portfolio_value_usd"], "110")
        self.assertEqual(rows[0]["total_account_value_usd"], "209.3341")
        self.assertEqual(rows[1]["balance"], "99.3341")
        self.assertEqual(rows[2]["title"], "Will this test pass?")

    async def test_reports_upstream_failure(self):
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = httpx.Response(
            429,
            request=httpx.Request("GET", "https://data-api.polymarket.com"),
        )

        with self.assertRaises(HTTPException) as context:
            await _fetch_positions(client, WALLET, 1)

        self.assertEqual(context.exception.status_code, 502)

    async def test_route_returns_cached_csv(self):
        rows = _parse_rows(WALLET, [_position()], Decimal("110"))
        with patch(
            "routers.polymarket._fetch_polymarket_rows",
            AsyncMock(return_value=rows),
        ):
            response = await get_polymarket_positions_csv(WALLET, 1)

        parsed = list(csv.DictReader(io.StringIO(response.body.decode())))
        self.assertEqual(len(parsed), 3)
        self.assertEqual(parsed[0]["position_id"], f"{WALLET}:portfolio")
        self.assertEqual(parsed[1]["position_id"], f"{WALLET}:pusd")
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")


if __name__ == "__main__":
    unittest.main()
