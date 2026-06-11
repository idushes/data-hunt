import unittest
from unittest.mock import AsyncMock, patch

from routers.solana import _fetch_market_infos


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
