import asyncio
import unittest

import httpx
from fastapi import FastAPI, Header
from fastapi.responses import PlainTextResponse, Response
from fastapi.testclient import TestClient

from csv_cache import CSVCacheMiddleware, CSVMemoryCacheMiddleware


class FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict[bytes, bytes]] = {}
        self.values: dict[str, bytes] = {}

    async def hgetall(self, key: str):
        return self.hashes.get(key, {}).copy()

    async def hset(self, key: str, mapping):
        self.hashes[key] = {
            str(field).encode(): value
            if isinstance(value, bytes)
            else str(value).encode()
            for field, value in mapping.items()
        }

    async def expire(self, key: str, seconds: int):
        return key in self.hashes

    def pipeline(self, transaction=True):
        return FakeRedisPipeline(self)

    async def set(self, key: str, value: str, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value.encode()
        return True

    async def exists(self, key: str):
        return key in self.values

    async def eval(self, script: str, key_count: int, key: str, token: str):
        if self.values.get(key) == token.encode():
            self.values.pop(key, None)
            return 1
        return 0


class FakeRedisPipeline:
    def __init__(self, redis: FakeRedis):
        self.redis = redis
        self.commands = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    def hset(self, key: str, mapping):
        self.commands.append(("hset", key, mapping))
        return self

    def expire(self, key: str, seconds: int):
        self.commands.append(("expire", key, seconds))
        return self

    async def execute(self):
        results = []
        for command, key, value in self.commands:
            if command == "hset":
                results.append(await self.redis.hset(key, value))
            else:
                results.append(await self.redis.expire(key, value))
        return results


def _build_app() -> tuple[FastAPI, dict[str, int]]:
    app = FastAPI()
    calls = {"csv": 0, "text": 0, "failure": 0}
    app.add_middleware(CSVMemoryCacheMiddleware, ttl_seconds=60, max_entries=16)

    @app.get("/report.csv")
    async def report(
        value: str = "default",
        authorization: str | None = Header(None),
    ):
        calls["csv"] += 1
        return Response(
            content=f"value,authorization\n{value},{authorization or ''}\n",
            media_type="text/csv",
        )

    @app.get("/number")
    async def number():
        calls["text"] += 1
        return PlainTextResponse(str(calls["text"]))

    @app.get("/failure.csv")
    async def failure():
        calls["failure"] += 1
        return Response(
            content="error\nupstream unavailable\n",
            status_code=502,
            media_type="text/csv",
        )

    return app, calls


class CSVMemoryCacheMiddlewareTest(unittest.TestCase):
    def test_caches_successful_csv_response(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get("/report.csv?value=one")
            second = client.get("/report.csv?value=one")

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(second.headers["x-csv-cache"], "HIT")
        self.assertEqual(first.text, second.text)
        self.assertEqual(calls["csv"], 1)

    def test_sheets_auth_token_does_not_fragment_shared_csv_cache(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get("/report.csv?value=one&auth_token=user-one")
            second = client.get("/report.csv?value=one&auth_token=user-two")

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(second.headers["x-csv-cache"], "HIT")
        self.assertEqual(calls["csv"], 1)

    def test_auth_headers_have_separate_cache_entries(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get(
                "/report.csv", headers={"Authorization": "Bearer user-one"}
            )
            second = client.get(
                "/report.csv", headers={"Authorization": "Bearer user-two"}
            )
            repeated = client.get(
                "/report.csv", headers={"Authorization": "Bearer user-one"}
            )

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(second.headers["x-csv-cache"], "MISS")
        self.assertEqual(repeated.headers["x-csv-cache"], "HIT")
        self.assertNotEqual(first.text, second.text)
        self.assertEqual(calls["csv"], 2)

    def test_cache_buster_does_not_bypass_server_cache(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get("/report.csv?refresh=1")
            second = client.get("/report.csv?refresh=2")

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(second.headers["x-csv-cache"], "HIT")
        self.assertEqual(calls["csv"], 1)

    def test_query_parameter_order_does_not_create_duplicate_entries(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get("/report.csv?value=one&network=ethereum")
            second = client.get("/report.csv?network=ethereum&value=one")

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(second.headers["x-csv-cache"], "HIT")
        self.assertEqual(calls["csv"], 1)

    def test_failed_csv_response_is_never_cached(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get("/failure.csv")
            second = client.get("/failure.csv")

        self.assertEqual(first.status_code, 502)
        self.assertEqual(second.status_code, 502)
        self.assertNotIn("x-csv-cache", first.headers)
        self.assertNotIn("x-csv-cache", second.headers)
        self.assertEqual(calls["failure"], 2)

    def test_does_not_cache_non_csv_response(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get("/number")
            second = client.get("/number")

        self.assertEqual(first.text, "1")
        self.assertEqual(second.text, "2")
        self.assertNotIn("x-csv-cache", second.headers)
        self.assertEqual(calls["text"], 2)


class CSVMemoryCacheSingleFlightTest(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_cache_misses_share_one_csv_request(self):
        app = FastAPI()
        calls = 0
        arrivals = 0
        all_arrived = asyncio.Event()
        request_count = 8

        app.add_middleware(
            CSVMemoryCacheMiddleware,
            ttl_seconds=60,
            max_entries=16,
        )

        @app.middleware("http")
        async def count_arrivals(request, call_next):
            nonlocal arrivals
            arrivals += 1
            if arrivals == request_count:
                all_arrived.set()
            return await call_next(request)

        @app.get("/slow.csv")
        async def slow_csv():
            nonlocal calls
            calls += 1
            await asyncio.wait_for(all_arrived.wait(), timeout=1)
            return Response(content="id,value\none,42\n", media_type="text/csv")

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            responses = await asyncio.gather(
                *(client.get("/slow.csv") for _ in range(request_count))
            )

        self.assertEqual(calls, 1)
        self.assertTrue(
            all(response.text == "id,value\none,42\n" for response in responses)
        )
        cache_results = [response.headers["x-csv-cache"] for response in responses]
        self.assertEqual(cache_results.count("MISS"), 1)
        self.assertEqual(cache_results.count("HIT"), request_count - 1)


class CSVRedisCacheTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _redis_app(redis: FakeRedis, calls: dict[str, int]) -> FastAPI:
        app = FastAPI()
        app.add_middleware(
            CSVCacheMiddleware,
            ttl_seconds=60,
            max_entries=16,
            redis_client=redis,
        )

        @app.get("/shared.csv")
        async def shared_csv():
            calls["count"] += 1
            return Response(content="value\n42\n", media_type="text/csv")

        return app

    async def test_cache_is_shared_between_application_instances(self):
        redis = FakeRedis()
        first_calls = {"count": 0}
        second_calls = {"count": 0}
        first_app = self._redis_app(redis, first_calls)
        second_app = self._redis_app(redis, second_calls)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=first_app),
            base_url="http://first",
        ) as first_client:
            first = await first_client.get("/shared.csv")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=second_app),
            base_url="http://second",
        ) as second_client:
            second = await second_client.get("/shared.csv")

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(first.headers["x-csv-cache-backend"], "redis")
        self.assertEqual(second.headers["x-csv-cache"], "HIT")
        self.assertEqual(second.headers["x-csv-cache-backend"], "redis")
        self.assertEqual(first_calls["count"], 1)
        self.assertEqual(second_calls["count"], 0)

    async def test_distributed_single_flight_coalesces_instances(self):
        redis = FakeRedis()
        calls = {"count": 0}

        def build_app() -> FastAPI:
            app = FastAPI()
            app.add_middleware(
                CSVCacheMiddleware,
                ttl_seconds=60,
                max_entries=16,
                redis_client=redis,
            )

            @app.get("/slow.csv")
            async def slow_csv():
                calls["count"] += 1
                await asyncio.sleep(0.1)
                return Response(content="value\n42\n", media_type="text/csv")

            return app

        first_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=build_app()),
            base_url="http://first",
        )
        second_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=build_app()),
            base_url="http://second",
        )
        try:
            first, second = await asyncio.gather(
                first_client.get("/slow.csv"),
                second_client.get("/slow.csv"),
            )
        finally:
            await first_client.aclose()
            await second_client.aclose()

        self.assertEqual(calls["count"], 1)
        self.assertEqual(
            sorted([first.headers["x-csv-cache"], second.headers["x-csv-cache"]]),
            ["HIT", "MISS"],
        )


if __name__ == "__main__":
    unittest.main()
