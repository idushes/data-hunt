import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from urllib.parse import parse_qsl

import jwt
from fastapi import Request, Response, status
from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse

from config import (
    ALGORITHM,
    SECRET_KEY,
    VALUE_RATE_LIMIT_ANONYMOUS,
    VALUE_RATE_LIMIT_AUTHENTICATED,
    VALUE_RATE_LIMIT_WINDOW_SECONDS,
)
from database import SessionLocal
from models import AccountToken
from redis_client import get_redis_client


logger = logging.getLogger(__name__)

INCREMENT_RATE_LIMIT_SCRIPT = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('TTL', KEYS[1])
return {count, ttl}
"""


@dataclass(frozen=True)
class RateLimitStatus:
    limit: int
    remaining: int
    retry_after: int
    exceeded: bool


class ValueRateLimitMiddleware(BaseHTTPMiddleware):
    """Rate-limit saved-value reads and registrations by account or client IP."""

    def __init__(
        self,
        app,
        authenticated_limit: int = VALUE_RATE_LIMIT_AUTHENTICATED,
        anonymous_limit: int = VALUE_RATE_LIMIT_ANONYMOUS,
        window_seconds: int = VALUE_RATE_LIMIT_WINDOW_SECONDS,
        redis_client: Redis | None = None,
        session_factory=SessionLocal,
    ):
        super().__init__(app)
        self.authenticated_limit = max(1, authenticated_limit)
        self.anonymous_limit = max(1, anonymous_limit)
        self.window_seconds = max(1, window_seconds)
        self._redis_client = redis_client
        self._session_factory = session_factory
        self._memory: dict[str, tuple[int, float]] = {}
        self._memory_lock = asyncio.Lock()
        self._last_redis_warning = 0.0

    @staticmethod
    def _is_limited_request(request: Request) -> bool:
        path = request.url.path.rstrip("/") or "/"
        method = request.method.upper()
        return (method == "GET" and (path == "/value" or path.startswith("/v/"))) or (
            method == "POST" and path == "/value-resources"
        )

    @staticmethod
    def _query_token(request: Request) -> tuple[bool, str]:
        for name, value in parse_qsl(request.url.query, keep_blank_values=True):
            if name == "auth_token":
                return True, value
        return False, ""

    @staticmethod
    def _bearer_token(request: Request) -> tuple[bool, str]:
        header = request.headers.get("authorization")
        if header is None:
            return False, ""
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or not token:
            return True, ""
        return True, token.strip()

    def _account_identity(self, request: Request) -> str | None:
        query_supplied, query_token = self._query_token(request)
        header_supplied, header_token = self._bearer_token(request)
        if query_supplied:
            token = query_token
            expected_purpose = "sheets"
        elif header_supplied:
            token = header_token
            expected_purpose = "session"
        else:
            return None

        if not token:
            raise ValueError("Invalid access token")
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        except jwt.PyJWTError as exc:
            raise ValueError("Invalid or expired access token") from exc

        account_id = payload.get("sub")
        token_id = payload.get("jti")
        if not isinstance(account_id, str) or not isinstance(token_id, str):
            raise ValueError("Invalid access token")
        if expected_purpose == "sheets" and payload.get("scope") != "sheets":
            raise ValueError("A scoped Sheets token is required")
        if expected_purpose == "session" and payload.get("scope") == "sheets":
            raise ValueError("A login token is required")

        with self._session_factory() as db:
            stored_token = (
                db.query(AccountToken)
                .filter(
                    AccountToken.id == token_id,
                    AccountToken.account_id == account_id,
                    AccountToken.purpose == expected_purpose,
                    AccountToken.is_active.is_(True),
                )
                .first()
            )
        if stored_token is None:
            raise ValueError("Access token is invalid or revoked")
        return account_id

    @staticmethod
    def _anonymous_identity(request: Request) -> str:
        forwarded_for = request.headers.get("x-forwarded-for", "")
        # Nginx appends the directly connected client to the right-hand side.
        # Using that value prevents a caller-supplied left-most address from
        # choosing an arbitrary rate-limit bucket.
        host = forwarded_for.rsplit(",", 1)[-1].strip()
        if not host:
            host = request.client.host if request.client else "unknown"
        return f"ip:{host}"

    @staticmethod
    def _rate_key(identity: str) -> str:
        digest = hashlib.sha256(identity.encode()).hexdigest()
        return f"datahunt:rate:value:v1:{digest}"

    def _redis(self) -> Redis | None:
        return self._redis_client or get_redis_client()

    def _warn_redis(self, exc: Exception) -> None:
        now = time.monotonic()
        if now - self._last_redis_warning >= 30:
            logger.warning(
                "Redis value rate limiter unavailable; using memory fallback: %s",
                exc,
            )
            self._last_redis_warning = now

    async def _increment_memory(self, key: str, limit: int) -> RateLimitStatus:
        now = time.monotonic()
        async with self._memory_lock:
            count, expires_at = self._memory.get(key, (0, now + self.window_seconds))
            if expires_at <= now:
                count = 0
                expires_at = now + self.window_seconds
            count += 1
            self._memory[key] = (count, expires_at)
        retry_after = max(1, int(expires_at - now + 0.999))
        return RateLimitStatus(
            limit=limit,
            remaining=max(0, limit - count),
            retry_after=retry_after,
            exceeded=count > limit,
        )

    async def _increment(self, key: str, limit: int) -> RateLimitStatus:
        client = self._redis()
        if client is not None:
            try:
                result = await client.eval(
                    INCREMENT_RATE_LIMIT_SCRIPT,
                    1,
                    key,
                    self.window_seconds,
                )
                count = int(result[0])
                ttl = max(1, int(result[1]))
                return RateLimitStatus(
                    limit=limit,
                    remaining=max(0, limit - count),
                    retry_after=ttl,
                    exceeded=count > limit,
                )
            except (RedisError, TypeError, ValueError) as exc:
                self._warn_redis(exc)
        return await self._increment_memory(key, limit)

    @staticmethod
    def _headers(rate: RateLimitStatus) -> dict[str, str]:
        return {
            "X-RateLimit-Limit": str(rate.limit),
            "X-RateLimit-Remaining": str(rate.remaining),
            "X-RateLimit-Reset": str(rate.retry_after),
        }

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        if not self._is_limited_request(request):
            return await call_next(request)

        try:
            account_id = self._account_identity(request)
        except ValueError as exc:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": str(exc)},
            )

        if account_id is None:
            identity = self._anonymous_identity(request)
            limit = self.anonymous_limit
        else:
            identity = f"account:{account_id}"
            limit = self.authenticated_limit

        rate = await self._increment(self._rate_key(identity), limit)
        headers = self._headers(rate)
        if rate.exceeded:
            headers["Retry-After"] = str(rate.retry_after)
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "detail": "Rate limit exceeded. Try again later or sign in for a higher limit."
                },
                headers=headers,
            )

        response = await call_next(request)
        for name, value in headers.items():
            response.headers[name] = value
        return response
