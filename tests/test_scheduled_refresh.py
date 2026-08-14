import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from csv_cache import CSVCacheMiddleware
from database import Base
from models import ValueResource
from routers.value import (
    RESOURCE_CREDENTIAL_PARAMS,
    VALUE_SOURCES,
    ValueSource,
)
from scheduled_refresh import (
    REFRESH_QUEUE_KEY,
    ScheduledRefreshMiddleware,
    ScheduledRefreshQueue,
    scheduled_refresh,
)
from value_rate_limit import (
    DATA_ACCESS_INTERNAL_HEADER,
    DATA_ACCESS_INTERNAL_TOKEN,
)


class FakeRefreshRedis:
    def __init__(self):
        self.hashes: dict[str, dict[bytes, bytes]] = {}
        self.values: dict[str, bytes] = {}
        self.zsets: dict[str, dict[bytes, float]] = {}
        self.expirations: dict[str, int] = {}

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
        self.expirations[key] = seconds
        return True

    async def set(self, key: str, value: str, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value.encode()
        if ex is not None:
            self.expirations[key] = ex
        return True

    async def get(self, key: str):
        return self.values.get(key)

    async def exists(self, key: str):
        return key in self.values

    async def zadd(self, key: str, mapping, nx=False):
        target = self.zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            encoded = member.encode() if isinstance(member, str) else member
            if nx and encoded in target:
                continue
            added += encoded not in target
            target[encoded] = float(score)
        return added

    async def zcard(self, key: str):
        return len(self.zsets.get(key, {}))

    async def zcount(self, key: str, minimum, maximum):
        low = float("-inf") if minimum == "-inf" else float(minimum)
        high = float(maximum)
        return sum(
            low <= score <= high for score in self.zsets.get(key, {}).values()
        )

    async def eval(self, script: str, key_count: int, key: str, *args):
        if "ZRANGEBYSCORE" in script:
            maximum = float(args[0])
            due = [
                (score, member)
                for member, score in self.zsets.get(key, {}).items()
                if score <= maximum
            ]
            if not due:
                return None
            _, member = min(due)
            self.zsets[key].pop(member, None)
            return member
        token = str(args[0]).encode()
        if self.values.get(key) == token:
            self.values.pop(key, None)
            return 1
        return 0

    def pipeline(self, transaction=True):
        return FakeRefreshPipeline(self)


class FakeRefreshPipeline:
    def __init__(self, redis: FakeRefreshRedis):
        self.redis = redis
        self.commands = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    def hset(self, key: str, mapping):
        self.commands.append(("hset", (key, mapping), {}))
        return self

    def expire(self, key: str, seconds: int):
        self.commands.append(("expire", (key, seconds), {}))
        return self

    def set(self, key: str, value: str, **kwargs):
        self.commands.append(("set", (key, value), kwargs))
        return self

    def zadd(self, key: str, mapping, **kwargs):
        self.commands.append(("zadd", (key, mapping), kwargs))
        return self

    async def execute(self):
        results = []
        for method, args, kwargs in self.commands:
            results.append(await getattr(self.redis, method)(*args, **kwargs))
        return results


class ScheduledRefreshTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def tearDown(self):
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _resource(
        self,
        resource_id: str,
        fingerprint: str,
        source: str = "morpho",
        parameters: dict[str, str] | None = None,
    ) -> None:
        with self.Session() as db:
            db.add(
                ValueResource(
                    id=resource_id,
                    fingerprint=fingerprint,
                    source=source,
                    key="position",
                    column="supply_usd",
                    parameters=parameters or {"chain_id": "1"},
                    created_at=1,
                )
            )
            db.commit()

    async def test_deduplicates_cells_from_one_source_without_storing_parameters(self):
        self._resource("ResourceOne1", "1" * 64)
        self._resource("ResourceTwo2", "2" * 64)
        redis = FakeRefreshRedis()
        queue = ScheduledRefreshQueue(
            redis_client=redis,
            session_factory=self.Session,
            delay_seconds=3540,
        )

        first = await queue.schedule("ResourceOne1")
        second = await queue.schedule("ResourceTwo2")

        self.assertTrue(first)
        self.assertTrue(second)
        self.assertEqual(len(redis.zsets[REFRESH_QUEUE_KEY]), 1)
        stored = repr(redis.values) + repr(redis.zsets)
        self.assertNotIn("chain_id", stored)
        self.assertNotIn("auth_token", stored)

    async def test_credential_source_is_not_scheduled(self):
        self._resource("CoinbaseRes1", "3" * 64, source="coinbase")
        redis = FakeRefreshRedis()
        queue = ScheduledRefreshQueue(
            redis_client=redis,
            session_factory=self.Session,
        )

        scheduled = await queue.schedule("CoinbaseRes1")

        self.assertFalse(scheduled)
        self.assertNotIn(REFRESH_QUEUE_KEY, redis.zsets)

    async def test_due_refresh_forces_source_cache_update(self):
        self._resource(
            "RefreshTest1",
            "4" * 64,
            source="refresh-test",
            parameters={"wallet": "public-wallet"},
        )
        redis = FakeRefreshRedis()
        app = FastAPI()
        value = "old"
        calls = 0
        app.add_middleware(CSVCacheMiddleware, redis_client=redis)

        @app.get("/refresh-test.csv")
        async def refresh_test_csv(wallet: str):
            nonlocal calls
            calls += 1
            return Response(
                content=f"wallet,value\n{wallet},{value}\n",
                media_type="text/csv",
            )

        queue = ScheduledRefreshQueue(
            redis_client=redis,
            session_factory=self.Session,
        )
        queue._app = app

        with patch.dict(
            VALUE_SOURCES,
            {"refresh-test": ValueSource("/refresh-test.csv", "wallet")},
        ), patch.dict(
            RESOURCE_CREDENTIAL_PARAMS,
            {"refresh-test": frozenset()},
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                first = await client.get(
                    "/refresh-test.csv", params={"wallet": "public-wallet"}
                )
                value = "new"
                refreshed = await queue._refresh_resource("RefreshTest1")
                repeated = await client.get(
                    "/refresh-test.csv", params={"wallet": "public-wallet"}
                )

        self.assertTrue(refreshed)
        self.assertEqual(first.text, "wallet,value\npublic-wallet,old\n")
        self.assertEqual(repeated.text, "wallet,value\npublic-wallet,new\n")
        self.assertEqual(repeated.headers["x-csv-cache"], "HIT")
        self.assertEqual(calls, 2)


class ScheduledRefreshMiddlewareTest(unittest.TestCase):
    def test_schedules_only_successful_external_credential_free_short_reads(self):
        app = FastAPI()
        app.add_middleware(ScheduledRefreshMiddleware)

        @app.get("/v/{resource_id}")
        async def value(resource_id: str):
            return Response(content=resource_id, media_type="text/csv")

        schedule = AsyncMock(return_value=True)
        with patch.object(scheduled_refresh, "schedule", schedule):
            with TestClient(app) as client:
                safe = client.get(
                    "/v/AbCdEf123456", params={"auth_token": "redacted"}
                )
                client.get(
                    "/v/AbCdEf123456",
                    params={
                        "auth_token": "redacted",
                        "capsule": "encrypted-secret",
                    },
                )
                client.get(
                    "/v/AbCdEf123456",
                    headers={
                        DATA_ACCESS_INTERNAL_HEADER: DATA_ACCESS_INTERNAL_TOKEN
                    },
                )

        self.assertEqual(safe.status_code, 200)
        schedule.assert_awaited_once_with("AbCdEf123456")


if __name__ == "__main__":
    unittest.main()
