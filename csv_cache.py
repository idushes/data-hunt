import hashlib
import time
from collections import OrderedDict
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import StreamingResponse


CACHE_BUSTER_PARAMS = {"_", "cache_bust", "refresh"}


@dataclass(frozen=True)
class CachedCSVResponse:
    expires_at: float
    body: bytes
    status_code: int
    raw_headers: tuple[tuple[bytes, bytes], ...]


class CSVMemoryCacheMiddleware(BaseHTTPMiddleware):
    """Caches successful GET CSV responses in process memory."""

    def __init__(self, app, ttl_seconds: int = 60, max_entries: int = 256):
        super().__init__(app)
        self.ttl_seconds = max(60, ttl_seconds)
        self.max_entries = max(1, max_entries)
        self._cache: OrderedDict[str, CachedCSVResponse] = OrderedDict()

    @staticmethod
    def _cache_key(request: Request) -> str:
        query_items = [
            (key, value)
            for key, value in parse_qsl(
                request.url.query, keep_blank_values=True
            )
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

    def _get(self, key: str) -> CachedCSVResponse | None:
        cached = self._cache.get(key)
        if cached is None:
            return None
        if cached.expires_at <= time.monotonic():
            self._cache.pop(key, None)
            return None
        self._cache.move_to_end(key)
        return cached

    def _set(self, key: str, cached: CachedCSVResponse) -> None:
        self._cache[key] = cached
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_entries:
            self._cache.popitem(last=False)

    @staticmethod
    def _response_from_cache(cached: CachedCSVResponse) -> Response:
        response = Response(content=cached.body, status_code=cached.status_code)
        response.raw_headers = list(cached.raw_headers)
        response.headers["X-CSV-Cache"] = "HIT"
        return response

    @staticmethod
    async def _read_body(response: StreamingResponse) -> bytes:
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk.encode() if isinstance(chunk, str) else chunk)
        return b"".join(chunks)

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if request.method.upper() != "GET":
            return await call_next(request)

        key = self._cache_key(request)
        cached = self._get(key)
        if cached is not None:
            return self._response_from_cache(cached)

        response = await call_next(request)
        content_type = response.headers.get("content-type", "").lower()
        if response.status_code != 200 or not content_type.startswith("text/csv"):
            return response

        body = await self._read_body(response)
        raw_headers = tuple(
            (key, value)
            for key, value in response.raw_headers
            if key.lower() != b"x-csv-cache"
        )
        self._set(
            key,
            CachedCSVResponse(
                expires_at=time.monotonic() + self.ttl_seconds,
                body=body,
                status_code=response.status_code,
                raw_headers=raw_headers,
            ),
        )

        fresh_response = Response(
            content=body,
            status_code=response.status_code,
            background=response.background,
        )
        fresh_response.raw_headers = list(raw_headers)
        fresh_response.headers["X-CSV-Cache"] = "MISS"
        return fresh_response
