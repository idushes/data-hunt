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
    _format,
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


class CompoundFetchTest(unittest.IsolatedAsyncioTestCase):
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
