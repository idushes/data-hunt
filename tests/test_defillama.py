import asyncio
import time
import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from routers import defillama


COIN = "coingecko:pax-gold"
NOW = 1_800_000_000


class DefiLlamaIdentifiersTest(unittest.TestCase):
    def test_existing_saved_tickers_and_explicit_ids_resolve_consistently(self):
        for ticker, identifier, legacy_id in (
            ("PAXG", "pax-gold", 4705),
            ("MON", "monad", 30495),
            ("wstETH", "wrapped-steth", 12409),
        ):
            with self.subTest(ticker=ticker):
                expected = f"coingecko:{identifier}"
                self.assertEqual(defillama._resolve_coin(None, ticker, None), expected)
                self.assertEqual(defillama._resolve_coin(None, None, legacy_id), expected)
                self.assertEqual(defillama._resolve_coin(ticker, None, None), expected)
                self.assertEqual(defillama._resolve_coin(identifier, None, None), expected)

    def test_exact_coin_takes_precedence_over_legacy_selectors(self):
        self.assertEqual(defillama._resolve_coin("pax-gold", "ETH", 1), COIN)

    def test_address_case_is_normalized_only_for_evm(self):
        address = "0x" + "Ab" * 20
        self.assertEqual(
            defillama._normalize_coin(f" Ethereum:{address} "),
            f"ethereum:{address.lower()}",
        )
        self.assertEqual(
            defillama._normalize_coin("Solana:CaseSensitiveMint123"),
            "solana:CaseSensitiveMint123",
        )

    def test_rejects_missing_unknown_legacy_or_malformed_selectors(self):
        for coin, symbol, token_id in (
            (None, None, None), (None, "UNKNOWN", None), (None, None, 99999),
            ("123", None, None), ("pax-gold,bitcoin", None, None),
            ("ethereum:0x123", None, None), ("https://example.com", None, None),
            ("../bitcoin", None, None), ("coingecko:", None, None),
        ):
            with self.subTest(coin=coin, symbol=symbol, token_id=token_id):
                with self.assertRaises(HTTPException) as raised:
                    defillama._resolve_coin(coin, symbol, token_id)
                self.assertEqual(raised.exception.status_code, 400)


class DefiLlamaFetchTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        defillama._price_cache.clear()
        defillama._inflight.clear()
        self.clock = patch("routers.defillama.time.time", return_value=NOW)
        self.clock.start()
        self.client = AsyncMock()

        @asynccontextmanager
        async def client_context(**kwargs):
            yield self.client

        self.transport = patch("routers.defillama.queued_async_client", client_context)
        self.transport.start()

    def tearDown(self):
        self.transport.stop()
        self.clock.stop()
        defillama._price_cache.clear()
        defillama._inflight.clear()

    def respond(self, row=None, *, status=200):
        row = row if row is not None else {
            "price": 2500.25, "timestamp": NOW - 1200, "confidence": 0.99,
        }
        self.client.get.return_value = httpx.Response(
            status, json={"coins": {COIN: row}}
        )

    async def test_uses_keyless_price_endpoint_and_source_timestamp(self):
        self.respond()
        price = await defillama._get_price(COIN)
        self.assertEqual(price, defillama.Price(2500.25, NOW - 1200))
        self.client.get.assert_awaited_once_with(
            "https://coins.llama.fi/prices/current/coingecko:pax-gold",
            headers={"Accept": "application/json"},
        )
        self.assertEqual(await defillama._get_price(COIN), price)
        self.assertEqual(self.client.get.await_count, 1)

    async def test_rejects_unusable_provider_responses_without_caching(self):
        cases = (
            ([], 502), ({}, 502), ({"coins": []}, 502), ({"coins": {}}, 404),
            ({"coins": {COIN: []}}, 502),
            ({"coins": {COIN: {"price": 0, "timestamp": NOW}}}, 502),
            ({"coins": {COIN: {"price": -1, "timestamp": NOW}}}, 502),
            ({"coins": {COIN: {"price": True, "timestamp": NOW}}}, 502),
            ({"coins": {COIN: {"price": "100", "timestamp": NOW}}}, 502),
            ({"coins": {COIN: {"price": 10**400, "timestamp": NOW}}}, 502),
            ({"coins": {COIN: {"price": 1}}}, 502),
            ({"coins": {COIN: {"price": 1, "timestamp": False}}}, 502),
            ({"coins": {COIN: {"price": 1, "timestamp": NOW - 3600}}}, 503),
            ({"coins": {COIN: {"price": 1, "timestamp": NOW + 61}}}, 503),
            ({"coins": {COIN: {"price": 1, "timestamp": NOW, "confidence": 0.49}}}, 503),
            ({"coins": {COIN: {"price": 1, "timestamp": NOW, "confidence": 2}}}, 503),
        )
        for payload, expected in cases:
            with self.subTest(payload=payload):
                self.client.get.return_value = httpx.Response(200, json=payload)
                with self.assertRaises(HTTPException) as raised:
                    await defillama._get_price(COIN)
                self.assertEqual(raised.exception.status_code, expected)
                self.assertNotIn(COIN, defillama._price_cache)
                self.assertNotIn(COIN, defillama._inflight)

    async def test_rejects_invalid_json_and_nonfinite_prices(self):
        for body in (
            b"not-json",
            b'{"coins":{"coingecko:pax-gold":{"price":NaN,"timestamp":1800000000}}}',
            b'{"coins":{"coingecko:pax-gold":{"price":Infinity,"timestamp":1800000000}}}',
        ):
            self.client.get.return_value = httpx.Response(200, content=body)
            with self.assertRaises(HTTPException) as raised:
                await defillama._get_price(COIN)
            self.assertEqual(raised.exception.status_code, 502)

    async def test_confidence_is_optional(self):
        self.respond({"price": 100, "timestamp": NOW})
        self.assertEqual((await defillama._get_price(COIN)).value, 100)

    async def test_provider_errors_are_translated_and_can_be_retried(self):
        for upstream, expected in ((429, 503), (500, 502), (403, 502)):
            self.respond(status=upstream)
            with self.assertRaises(HTTPException) as raised:
                await defillama._get_price(COIN)
            self.assertEqual(raised.exception.status_code, expected)
        for exception, expected in (
            (httpx.ReadTimeout("timeout"), 504),
            (httpx.ConnectError("unavailable"), 502),
        ):
            self.client.get.side_effect = exception
            with self.assertRaises(HTTPException) as raised:
                await defillama._get_price(COIN)
            self.assertEqual(raised.exception.status_code, expected)
        self.client.get.side_effect = None
        self.respond()
        self.assertEqual((await defillama._get_price(COIN)).value, 2500.25)

    async def test_cache_never_extends_source_age_past_one_hour(self):
        self.respond({"price": 1, "timestamp": NOW - 3590})
        await defillama._get_price(COIN)
        expiry, _ = defillama._price_cache[COIN]
        self.assertLessEqual(expiry - time.monotonic(), 10)
        with patch("routers.defillama.time.time", return_value=NOW + 11):
            self.assertIsNone(defillama._cached_price(COIN))

    async def test_cache_is_bounded_and_expired_entries_are_refetched(self):
        with patch("routers.defillama.MAX_CACHE_ENTRIES", 2):
            for coin in ("coingecko:bitcoin", "coingecko:ethereum", COIN):
                self.client.get.return_value = httpx.Response(200, json={
                    "coins": {coin: {"price": 1, "timestamp": NOW}},
                })
                await defillama._get_price(coin)
            self.assertEqual(len(defillama._price_cache), 2)
            self.assertNotIn("coingecko:bitcoin", defillama._price_cache)
        defillama._price_cache[COIN] = (time.monotonic() - 1, defillama.Price(1, NOW))
        self.respond()
        self.assertEqual((await defillama._get_price(COIN)).value, 2500.25)

    async def test_simultaneous_requests_share_one_call_even_if_a_waiter_disconnects(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def fetch(*args, **kwargs):
            started.set()
            await release.wait()
            return httpx.Response(200, json={
                "coins": {COIN: {"price": 100, "timestamp": NOW}},
            })

        self.client.get.side_effect = fetch
        requests = [asyncio.create_task(defillama._get_price(COIN)) for _ in range(12)]
        await started.wait()
        requests[0].cancel()
        with self.assertRaises(asyncio.CancelledError):
            await requests[0]
        release.set()
        prices = await asyncio.gather(*requests[1:])
        self.assertEqual(prices, [defillama.Price(100, NOW)] * 11)
        self.assertEqual(self.client.get.await_count, 1)
        self.assertEqual(defillama._inflight, {})


class DefiLlamaRouteTest(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(defillama.router)
        self.client = TestClient(app)
        self.fetch = AsyncMock(return_value=defillama.Price(42.5, NOW))
        self.mock = patch("routers.defillama._get_price", self.fetch)
        self.mock.start()

    def tearDown(self):
        self.mock.stop()
        self.client.close()

    def test_legacy_route_and_new_coin_both_return_scalar_csv_and_source_time(self):
        for params in ({"symbol": "PAXG", "convert": "USD"}, {"coin": "pax-gold"}):
            response = self.client.get("/cmc/price.csv", params=params)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.text, "42.5")
            self.assertTrue(response.headers["content-type"].startswith("text/csv"))
            self.assertEqual(response.headers["x-data-updated-at"], str(NOW))
            self.fetch.assert_awaited_with(COIN)

    def test_unsupported_currency_fails_before_requesting_provider(self):
        response = self.client.get("/cmc/price.csv?symbol=PAXG&convert=EUR")
        self.assertEqual(response.status_code, 400)
        self.fetch.assert_not_awaited()
