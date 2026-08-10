import asyncio
import base64
import hashlib
import json
import logging
import secrets
import time
from collections import OrderedDict
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode

from fastapi import Request, Response
from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import StreamingResponse

from redis_client import get_redis_client

logger = logging.getLogger(__name__)

CACHE_BUSTER_PARAMS = {"_", "cache_bust", "refresh"}
RELEASE_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


@dataclass(frozen=True)
class CachedCSVResponse:
    expires_at: float
    body: bytes
    status_code: int
    raw_headers: tuple[tuple[bytes, bytes], ...]


class CSVCacheMiddleware(BaseHTTPMiddleware):
    """Caches successful GET CSV responses in Redis with distributed single-flight.

    A bounded in-memory cache and local single-flight are retained only as a safe
    fallback when Redis is not configured or temporarily unavailable.
    """

    def __init__(
        self,
        app,
        ttl_seconds: int = 60,
        max_entries: int = 256,
        flight_timeout_seconds: int = 180,
        redis_client: Redis | None = None,
    ):
        super().__init__(app)
        self.ttl_seconds = max(60, ttl_seconds)
        self.max_entries = max(1, max_entries)
        self.flight_timeout_seconds = max(30, flight_timeout_seconds)
        self._redis_client = redis_client
        self._cache: OrderedDict[str, CachedCSVResponse] = OrderedDict()
        self._inflight: dict[str, asyncio.Event] = {}
        self._inflight_lock = asyncio.Lock()
        self._last_redis_warning = 0.0

    @staticmethod
    def _cache_key(request: Request) -> str:
        query_items = [
            (key, value)
            for key, value in parse_qsl(request.url.query, keep_blank_values=True)
            if key.lower() not in CACHE_BUSTER_PARAMS
        ]
        query_items.sort()
        key_material = "\n".join(
            [
                request.method.upper(),
                request.url.path,
                urlencode(query_items, doseq=True),
                request.headers.get("authorization", ""),
                request.headers.get("cookie", ""),
                request.headers.get("origin", ""),
            ]
        )
        return hashlib.sha256(key_material.encode()).hexdigest()

    @staticmethod
    def _redis_cache_key(key: str) -> str:
        return f"datahunt:csv:v1:{key}"

    @staticmethod
    def _redis_lock_key(key: str) -> str:
        return f"datahunt:csv:lock:v1:{key}"

    def _redis(self) -> Redis | None:
        return self._redis_client or get_redis_client()

    def _warn_redis(self, exc: Exception) -> None:
        now = time.monotonic()
        if now - self._last_redis_warning >= 30:
            logger.warning(
                "Redis CSV cache unavailable; using memory fallback: %s", exc
            )
            self._last_redis_warning = now

    def _get_memory(self, key: str) -> CachedCSVResponse | None:
        cached = self._cache.get(key)
        if cached is None:
            return None
        if cached.expires_at <= time.monotonic():
            self._cache.pop(key, None)
            return None
        self._cache.move_to_end(key)
        return cached

    def _set_memory(self, key: str, cached: CachedCSVResponse) -> None:
        self._cache[key] = cached
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_entries:
            self._cache.popitem(last=False)

    async def _get_redis(self, key: str) -> CachedCSVResponse | None:
        client = self._redis()
        if client is None:
            return None
        try:
            payload = await client.hgetall(self._redis_cache_key(key))
            if not payload:
                return None
            body = payload.get(b"body") or payload.get("body")
            status_code = payload.get(b"status_code") or payload.get("status_code")
            headers_payload = payload.get(b"headers") or payload.get("headers")
            if body is None or status_code is None or headers_payload is None:
                return None
            if isinstance(headers_payload, bytes):
                headers_payload = headers_payload.decode()
            encoded_headers = json.loads(headers_payload)
            raw_headers = tuple(
                (
                    base64.b64decode(header),
                    base64.b64decode(value),
                )
                for header, value in encoded_headers
            )
            return CachedCSVResponse(
                expires_at=float("inf"),
                body=body if isinstance(body, bytes) else body.encode(),
                status_code=int(status_code),
                raw_headers=raw_headers,
            )
        except (RedisError, TypeError, ValueError, json.JSONDecodeError) as exc:
            self._warn_redis(exc)
            return None

    async def _set_redis(self, key: str, cached: CachedCSVResponse) -> bool:
        client = self._redis()
        if client is None:
            return False
        headers_payload = json.dumps(
            [
                [
                    base64.b64encode(header).decode(),
                    base64.b64encode(value).decode(),
                ]
                for header, value in cached.raw_headers
            ],
            separators=(",", ":"),
        )
        try:
            redis_key = self._redis_cache_key(key)
            async with client.pipeline(transaction=True) as pipeline:
                pipeline.hset(
                    redis_key,
                    mapping={
                        "body": cached.body,
                        "status_code": str(cached.status_code),
                        "headers": headers_payload,
                    },
                )
                pipeline.expire(redis_key, self.ttl_seconds)
                await pipeline.execute()
            return True
        except RedisError as exc:
            self._warn_redis(exc)
            return False

    async def _acquire_redis_lock(self, key: str) -> str | None:
        client = self._redis()
        if client is None:
            return None
        token = secrets.token_hex(16)
        try:
            acquired = await client.set(
                self._redis_lock_key(key),
                token,
                nx=True,
                ex=self.flight_timeout_seconds,
            )
            return token if acquired else ""
        except RedisError as exc:
            self._warn_redis(exc)
            return None

    async def _release_redis_lock(self, key: str, token: str) -> None:
        client = self._redis()
        if client is None:
            return
        try:
            await client.eval(
                RELEASE_LOCK_SCRIPT,
                1,
                self._redis_lock_key(key),
                token,
            )
        except RedisError as exc:
            self._warn_redis(exc)

    async def _wait_for_redis_flight(self, key: str) -> CachedCSVResponse | None:
        client = self._redis()
        if client is None:
            return None
        deadline = time.monotonic() + self.flight_timeout_seconds
        delay = 0.05
        try:
            while time.monotonic() < deadline:
                await asyncio.sleep(delay)
                cached = await self._get_redis(key)
                if cached is not None:
                    return cached
                if not await client.exists(self._redis_lock_key(key)):
                    return None
                delay = min(0.25, delay * 1.5)
        except RedisError as exc:
            self._warn_redis(exc)
        return None

    @staticmethod
    def _response_from_cache(
        cached: CachedCSVResponse,
        backend: str,
    ) -> Response:
        response = Response(content=cached.body, status_code=cached.status_code)
        response.raw_headers = list(cached.raw_headers)
        response.headers["X-CSV-Cache"] = "HIT"
        response.headers["X-CSV-Cache-Backend"] = backend
        return response

    @staticmethod
    async def _read_body(response: StreamingResponse) -> bytes:
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk.encode() if isinstance(chunk, str) else chunk)
        return b"".join(chunks)

    async def _call_and_cache(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
        key: str,
        backend: str,
    ) -> Response:
        response = await call_next(request)
        content_type = response.headers.get("content-type", "").lower()
        if response.status_code != 200 or not content_type.startswith("text/csv"):
            return response

        body = await self._read_body(response)
        raw_headers = tuple(
            (header, value)
            for header, value in response.raw_headers
            if header.lower() not in {b"x-csv-cache", b"x-csv-cache-backend"}
        )
        cached = CachedCSVResponse(
            expires_at=time.monotonic() + self.ttl_seconds,
            body=body,
            status_code=response.status_code,
            raw_headers=raw_headers,
        )
        if backend == "redis":
            stored_in_redis = await self._set_redis(key, cached)
            if not stored_in_redis:
                self._set_memory(key, cached)
                backend = "memory"
        else:
            self._set_memory(key, cached)

        fresh_response = Response(
            content=body,
            status_code=response.status_code,
            background=response.background,
        )
        fresh_response.raw_headers = list(raw_headers)
        fresh_response.headers["X-CSV-Cache"] = "MISS"
        fresh_response.headers["X-CSV-Cache-Backend"] = backend
        return fresh_response

    async def _dispatch_memory(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
        key: str,
    ) -> Response:
        cached = self._get_memory(key)
        if cached is not None:
            return self._response_from_cache(cached, "memory")

        async with self._inflight_lock:
            cached = self._get_memory(key)
            if cached is not None:
                return self._response_from_cache(cached, "memory")
            flight = self._inflight.get(key)
            is_leader = flight is None
            if flight is None:
                flight = asyncio.Event()
                self._inflight[key] = flight

        if not is_leader:
            await flight.wait()
            cached = self._get_memory(key)
            if cached is not None:
                return self._response_from_cache(cached, "memory")
            return await call_next(request)

        try:
            return await self._call_and_cache(request, call_next, key, "memory")
        finally:
            async with self._inflight_lock:
                current_flight = self._inflight.pop(key, None)
                if current_flight is not None:
                    current_flight.set()

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        if request.method.upper() != "GET":
            return await call_next(request)

        key = self._cache_key(request)
        client = self._redis()
        if client is None:
            return await self._dispatch_memory(request, call_next, key)

        cached = await self._get_redis(key)
        if cached is not None:
            return self._response_from_cache(cached, "redis")

        lock_token = await self._acquire_redis_lock(key)
        if lock_token is None:
            return await self._dispatch_memory(request, call_next, key)
        if lock_token == "":
            cached = await self._wait_for_redis_flight(key)
            if cached is not None:
                return self._response_from_cache(cached, "redis")
            lock_token = await self._acquire_redis_lock(key)
            if not lock_token:
                return await self._dispatch_memory(request, call_next, key)

        try:
            return await self._call_and_cache(request, call_next, key, "redis")
        finally:
            await self._release_redis_lock(key, lock_token)


# Backward-compatible import for existing integrations and tests.
CSVMemoryCacheMiddleware = CSVCacheMiddleware
