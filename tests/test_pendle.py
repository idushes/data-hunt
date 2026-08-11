import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.pendle import (
    _fetch_pendle_rows,
    _normalize_wallet,
    _parse_rows,
    get_pendle_positions_csv,
)


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"


def _payload():
    return {
        "positions": [
            {
                "chainId": 1,
                "totalOpen": 1,
                "totalClosed": 1,
                "totalSy": 1,
                "openPositions": [
                    {
                        "marketId": "1-0xABC",
                        "pt": {"valuation": 12.5, "balance": "100"},
                        "yt": {
                            "valuation": 2.5,
                            "balance": "20",
                            "claimTokenAmounts": [
                                {"token": "1-0xReward", "amount": "7"},
                                {"token": "1-0xZero", "amount": "0"},
                            ],
                        },
                        "lp": {
                            "valuation": 35,
                            "balance": "300",
                            "activeBalance": "250",
                        },
                        "crossPtPositions": [{"spokePt": "0xSpoke", "balance": "9"}],
                    }
                ],
                "closedPositions": [
                    {
                        "marketId": "1-0xClosed",
                        "pt": {"valuation": 0, "balance": "0"},
                        "yt": {"valuation": 0, "balance": "0"},
                        "lp": {
                            "valuation": 0,
                            "balance": "0",
                            "activeBalance": "0",
                        },
                        "crossPtPositions": [],
                    }
                ],
                "syPositions": [
                    {
                        "syId": "1-0xSY",
                        "balance": "55",
                        "claimTokenAmounts": [{"token": "1-0xReward2", "amount": "8"}],
                    }
                ],
                "updatedAt": "2026-08-11T00:00:00.000Z",
            }
        ]
    }


class PendleParserTest(unittest.TestCase):
    def test_normalizes_wallet(self):
        self.assertEqual(_normalize_wallet(WALLET.upper().replace("0X", "0x")), WALLET)
        with self.assertRaises(HTTPException):
            _normalize_wallet("not-an-address")

    def test_returns_summary_positions_and_claimable_rewards(self):
        rows = _parse_rows(WALLET, _payload(), False)

        self.assertEqual(rows[0]["position_id"], f"{WALLET}:portfolio")
        self.assertEqual(rows[0]["portfolio_value_usd"], "50")
        self.assertEqual(rows[0]["open_market_count"], "1")
        self.assertEqual(rows[0]["closed_market_count"], "1")
        self.assertEqual(rows[0]["claimable_reward_count"], "2")
        by_id = {row["position_id"]: row for row in rows}
        self.assertEqual(by_id["1-0xabc:lp"]["active_balance_raw"], "250")
        self.assertEqual(by_id["1-0xabc:pt"]["value_usd"], "12.5")
        self.assertEqual(
            by_id["1-0xabc:yt:reward:1-0xreward"]["claimable_amount_raw"],
            "7",
        )
        self.assertEqual(by_id["1-0xsy:sy"]["chain"], "Ethereum")
        self.assertNotIn("1-0xclosed:pt", by_id)

    def test_closed_positions_are_opt_in(self):
        rows = _parse_rows(WALLET, _payload(), True)
        by_id = {row["position_id"]: row for row in rows}

        self.assertEqual(by_id["1-0xclosed:pt"]["status"], "closed")
        self.assertEqual(by_id["1-0xclosed:lp"]["value_usd"], "0")

    def test_rejects_invalid_upstream_payload(self):
        with self.assertRaises(HTTPException) as context:
            _parse_rows(WALLET, {"positions": "invalid"}, False)
        self.assertEqual(context.exception.status_code, 502)


class PendleFetchTest(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_cross_chain_positions_once(self):
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = httpx.Response(
            200,
            json=_payload(),
            request=httpx.Request("GET", "https://api-v2.pendle.finance"),
        )

        rows = await _fetch_pendle_rows(client, WALLET, False)

        self.assertEqual(client.get.await_count, 1)
        self.assertEqual(rows[0]["portfolio_value_usd"], "50")
        self.assertEqual(client.get.await_args.kwargs["params"], {"filterUsd": 0})

    async def test_reports_upstream_failure(self):
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = httpx.Response(
            429,
            request=httpx.Request("GET", "https://api-v2.pendle.finance"),
        )

        with self.assertRaises(HTTPException) as context:
            await _fetch_pendle_rows(client, WALLET, False)

        self.assertEqual(context.exception.status_code, 502)

    async def test_route_returns_cached_csv(self):
        rows = _parse_rows(WALLET, _payload(), False)
        with patch(
            "routers.pendle._fetch_pendle_rows",
            AsyncMock(return_value=rows),
        ):
            response = await get_pendle_positions_csv(WALLET, False)

        parsed = list(csv.DictReader(io.StringIO(response.body.decode())))
        self.assertEqual(parsed[0]["position_id"], f"{WALLET}:portfolio")
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")


if __name__ == "__main__":
    unittest.main()
