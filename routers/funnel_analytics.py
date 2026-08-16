"""Anonymous, aggregate-only product funnel instrumentation."""

import asyncio
import hashlib
import hmac
import re
import secrets
import time
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from redis.exceptions import RedisError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from database import get_db
from models import AuthFunnelEvent
from redis_client import get_redis_client
from value_rate_limit import INCREMENT_RATE_LIMIT_SCRIPT


router = APIRouter(prefix="/analytics/funnel", tags=["anonymous analytics"])

AttributionValue = str | None
ATTRIBUTION_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]*$")


class FunnelEventInput(BaseModel):
    """Only anonymous session and coarse, allowlisted attribution are accepted."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    session_id: UUID
    event: Literal[
        "sheets_view",
        "login_clicked",
        "wallet_missing",
        "wallet_connection_rejected",
        "signature_requested",
        "signature_rejected",
        "login_succeeded",
        "login_failed",
        "table_loaded",
        "formula_copied",
    ]
    utm_source: AttributionValue = Field(default=None, max_length=48)
    utm_medium: AttributionValue = Field(default=None, max_length=48)
    utm_campaign: AttributionValue = Field(default=None, max_length=96)

    @field_validator("session_id")
    @classmethod
    def require_random_session_uuid(cls, value: UUID) -> UUID:
        if value.version != 4:
            raise ValueError("session_id must be a UUIDv4")
        return value

    @field_validator("utm_source", "utm_medium", "utm_campaign")
    @classmethod
    def validate_attribution(cls, value: AttributionValue) -> AttributionValue:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            return None
        if not ATTRIBUTION_PATTERN.fullmatch(normalized):
            raise ValueError("Attribution values use lowercase letters, numbers, ., _, or -")
        return normalized


class _AnonymousEventRateLimit:
    """Short-lived, HMAC-keyed rate limit; raw network addresses are never stored."""

    limit = 30
    window_seconds = 60

    def __init__(self) -> None:
        self._secret = secrets.token_bytes(32)
        self._memory: dict[str, tuple[int, float]] = {}
        self._lock = asyncio.Lock()

    def _key(self, request: Request) -> str:
        host = request.client.host if request.client else "unknown"
        digest = hmac.new(self._secret, host.encode(), hashlib.sha256).hexdigest()
        return f"datahunt:rate:funnel:v1:{digest}"

    async def _increment_memory(self, key: str) -> tuple[int, int]:
        now = time.monotonic()
        async with self._lock:
            count, expires_at = self._memory.get(key, (0, now + self.window_seconds))
            if expires_at <= now:
                count, expires_at = 0, now + self.window_seconds
            count += 1
            self._memory[key] = (count, expires_at)
        return count, max(1, int(expires_at - now + 0.999))

    async def check(self, request: Request) -> tuple[int, int]:
        key = self._key(request)
        redis = get_redis_client()
        if redis is not None:
            try:
                result = await redis.eval(
                    INCREMENT_RATE_LIMIT_SCRIPT, 1, key, self.window_seconds
                )
                return int(result[0]), max(1, int(result[1]))
            except (RedisError, TypeError, ValueError):
                # A local fallback keeps ingestion bounded if Redis is unavailable.
                pass
        return await self._increment_memory(key)


_rate_limiter = _AnonymousEventRateLimit()


@router.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def record_funnel_event(
    payload: FunnelEventInput,
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
):
    """Record one allowlisted event per anonymous session, event, and UTC day."""
    count, retry_after = await _rate_limiter.check(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-RateLimit-Limit"] = str(_rate_limiter.limit)
    response.headers["X-RateLimit-Remaining"] = str(max(0, _rate_limiter.limit - count))
    response.headers["X-RateLimit-Reset"] = str(retry_after)
    if count > _rate_limiter.limit:
        response.headers["Retry-After"] = str(retry_after)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Try again later.",
            headers=dict(response.headers),
        )

    item = AuthFunnelEvent(
        anonymous_session_id=str(payload.session_id),
        day=int(time.time()) // 86400,
        event_name=payload.event,
        utm_source=payload.utm_source,
        utm_medium=payload.utm_medium,
        utm_campaign=payload.utm_campaign,
    )
    db.add(item)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return {"accepted": True, "deduplicated": True}
    return {"accepted": True, "deduplicated": False}
