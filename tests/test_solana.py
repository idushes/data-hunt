import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from routers.solana import (
    _fetch_market_infos,
    _fetch_optional_lookup,
    _filter_positive_balance_items,
)


class FetchMarketInfosTest(unittest.IsolatedAsyncioTestCase):
    async def test_skips_graphql_query_without_mints(self):
        with patch("routers.solana._query_graphql", new_callable=AsyncMock) as query:
            result = await _fetch_market_infos(object(), [])

        self.assertEqual(result, {})
        query.assert_not_awaited()

    async def test_queries_only_requested_mints(self):
        with patch("routers.solana._query_graphql", new_callable=AsyncMock) as query:
            query.return_value = {
                "marketInfos": [
                    {"id": "mint-a", "name": "Market A"},
                    {"id": "mint-b", "name": "Market B"},
                ]
            }

            result = await _fetch_market_infos(object(), ["mint-a", "mint-b"])

        query.assert_awaited_once()
        gql_query = query.await_args.args[1]
        self.assertIn('where:{id_in:["mint-a","mint-b"]}', gql_query)
        self.assertEqual(result["mint-a"]["name"], "Market A")
        self.assertEqual(result["mint-b"]["name"], "Market B")


class PositiveBalanceItemsTest(unittest.TestCase):
    def test_filters_zero_empty_and_invalid_balances(self):
        result = _filter_positive_balance_items(
            [
                {"balance": "0", "id": "zero"},
                {"balance": None, "id": "missing"},
                {"balance": "not-a-number", "id": "invalid"},
                {"balance": "1", "id": "positive"},
            ]
        )

        self.assertEqual(result, [{"balance": "1", "id": "positive"}])


class OptionalLookupTest(unittest.IsolatedAsyncioTestCase):
    async def test_returns_empty_dict_on_bad_gateway(self):
        fetcher = AsyncMock(side_effect=HTTPException(status_code=502))

        result = await _fetch_optional_lookup(fetcher, object())

        self.assertEqual(result, {})

    async def test_reraises_non_bad_gateway_errors(self):
        fetcher = AsyncMock(side_effect=HTTPException(status_code=400))

        with self.assertRaises(HTTPException):
            await _fetch_optional_lookup(fetcher, object())
