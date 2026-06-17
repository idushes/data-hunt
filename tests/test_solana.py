import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.solana import (
    _gmtrade_csv_cache,
    _fetch_market_infos,
    _fetch_optional_lookup,
    _fetch_optional_positions,
    _filter_positive_balance_items,
    get_gmtrade_csv,
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


class OptionalPositionsTest(unittest.IsolatedAsyncioTestCase):
    async def test_returns_items_from_fetcher(self):
        fetcher = AsyncMock(return_value=[{"balance": "1"}])

        result = await _fetch_optional_positions(fetcher, object())

        self.assertEqual(result, [{"balance": "1"}])

    async def test_returns_empty_list_on_bad_gateway(self):
        fetcher = AsyncMock(side_effect=HTTPException(status_code=502))

        result = await _fetch_optional_positions(fetcher, object())

        self.assertEqual(result, [])

    async def test_returns_empty_list_on_timeout(self):
        async def slow_fetcher():
            await asyncio.sleep(0.05)
            return [{"balance": "1"}]

        with patch("routers.solana.OPTIONAL_POSITION_TIMEOUT", 0.001):
            result = await _fetch_optional_positions(slow_fetcher)

        self.assertEqual(result, [])

    async def test_reraises_non_bad_gateway_errors(self):
        fetcher = AsyncMock(side_effect=HTTPException(status_code=400))

        with self.assertRaises(HTTPException):
            await _fetch_optional_positions(fetcher, object())


class OptionalLookupTest(unittest.IsolatedAsyncioTestCase):
    async def test_returns_empty_dict_on_bad_gateway(self):
        fetcher = AsyncMock(side_effect=HTTPException(status_code=502))

        result = await _fetch_optional_lookup(fetcher, object())

        self.assertEqual(result, {})

    async def test_reraises_non_bad_gateway_errors(self):
        fetcher = AsyncMock(side_effect=HTTPException(status_code=400))

        with self.assertRaises(HTTPException):
            await _fetch_optional_lookup(fetcher, object())

    async def test_returns_empty_dict_on_timeout(self):
        async def slow_fetcher():
            await asyncio.sleep(0.05)
            return {"unexpected": True}

        with patch("routers.solana.OPTIONAL_LOOKUP_TIMEOUT", 0.001):
            result = await _fetch_optional_lookup(slow_fetcher)

        self.assertEqual(result, {})

    async def test_returns_empty_dict_on_httpx_errors(self):
        fetcher = AsyncMock(side_effect=httpx.ConnectError("network unavailable"))

        result = await _fetch_optional_lookup(fetcher, object())

        self.assertEqual(result, {})


class GmtradeCsvCacheTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _gmtrade_csv_cache.clear()

    def tearDown(self):
        _gmtrade_csv_cache.clear()

    async def test_updates_cache_after_successful_refresh(self):
        content = "type,mint\nGM,mint-a\n"

        with patch(
            "routers.solana._build_gmtrade_csv_content", new_callable=AsyncMock
        ) as build:
            build.return_value = content
            response = await get_gmtrade_csv(" wallet-a ")

        build.assert_awaited_once_with("wallet-a")
        self.assertEqual(response.body.decode(), content)
        self.assertEqual(_gmtrade_csv_cache["wallet-a"], content)

    async def test_returns_cached_csv_when_refresh_fails(self):
        cached = "type,mint\nGM,cached-mint\n"
        _gmtrade_csv_cache["wallet-a"] = cached

        with patch(
            "routers.solana._build_gmtrade_csv_content", new_callable=AsyncMock
        ) as build:
            build.side_effect = HTTPException(status_code=502)
            response = await get_gmtrade_csv("wallet-a")

        self.assertEqual(response.body.decode(), cached)

    async def test_returns_empty_csv_when_refresh_fails_without_cache(self):
        with patch(
            "routers.solana._build_gmtrade_csv_content", new_callable=AsyncMock
        ) as build:
            build.side_effect = HTTPException(status_code=502)
            response = await get_gmtrade_csv("wallet-a")

        self.assertEqual(
            response.body.decode(),
            (
                "type,mint,name,balance,price_usd,value_usd,long_token_mint,"
                "short_token_mint,index_token_mint,updated_at\r\n"
            ),
        )
