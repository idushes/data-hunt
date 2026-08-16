"""Anonymous, aggregate-only product funnel instrumentation."""

import asyncio
import hashlib
import re
import time
from typing import Literal, TypeAlias
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from redis.exceptions import RedisError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from database import get_db
from models import AuthFunnelEvent
from redis_client import get_redis_client
from value_rate_limit import INCREMENT_RATE_LIMIT_SCRIPT


router = APIRouter(prefix="/analytics/funnel", tags=["anonymous analytics"])

CampaignValue = str | None
AttributionSource: TypeAlias = Literal[
    "google",
    "reddit",
    "x",
    "threads",
    "product_hunt",
    "uneed",
    "launching_next",
]
AttributionMedium: TypeAlias = Literal["cpc", "organic", "social", "referral"]
CAMPAIGN_PATTERN = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+){0,3}$")


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
    utm_source: AttributionSource | None = None
    utm_medium: AttributionMedium | None = None
    utm_campaign: CampaignValue = Field(default=None, max_length=32)

    @field_validator("session_id")
    @classmethod
    def require_random_session_uuid(cls, value: UUID) -> UUID:
        if value.version != 4:
            raise ValueError("session_id must be a UUIDv4")
        return value

    @field_validator("utm_campaign")
    @classmethod
    def validate_campaign(cls, value: CampaignValue) -> CampaignValue:
        if value is None:
            return None
        if not CAMPAIGN_PATTERN.fullmatch(value):
            raise ValueError("utm_campaign must be a lowercase campaign slug")
        return value

    @model_validator(mode="after")
    def validate_attribution_pair(self):
        if (self.utm_source is None) != (self.utm_medium is None):
            raise ValueError("utm_source and utm_medium must be supplied together")
        if self.utm_campaign is not None and (
            self.utm_source != "google" or self.utm_medium != "cpc"
        ):
            raise ValueError("utm_campaign is only accepted for google cpc attribution")
        return self


class _AnonymousEventRateLimit:
    """Session and global quotas with no dependence on untrusted client IPs."""

    session_limit = 20
    global_limit = 240
    window_seconds = 60

    def __init__(self) -> None:
        self._memory: dict[str, tuple[int, float]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _session_key(session_id: UUID) -> str:
        digest = hashlib.sha256(str(session_id).encode()).hexdigest()
        return f"datahunt:rate:funnel:session:v1:{digest}"

    @staticmethod
    def _global_key() -> str:
        return "datahunt:rate:funnel:global:v1"

    async def _increment_memory(self, key: str) -> tuple[int, int]:
        now = time.monotonic()
        async with self._lock:
            count, expires_at = self._memory.get(key, (0, now + self.window_seconds))
            if expires_at <= now:
                count, expires_at = 0, now + self.window_seconds
            count += 1
            self._memory[key] = (count, expires_at)
        return count, max(1, int(expires_at - now + 0.999))

    async def check(self, session_id: UUID) -> tuple[int, int, int, int]:
        session_key = self._session_key(session_id)
        global_key = self._global_key()
        redis = get_redis_client()
        if redis is not None:
            try:
                result = await redis.eval(
                    INCREMENT_RATE_LIMIT_SCRIPT, 1, session_key, self.window_seconds
                )
                global_result = await redis.eval(
                    INCREMENT_RATE_LIMIT_SCRIPT, 1, global_key, self.window_seconds
                )
                return (
                    int(result[0]),
                    max(1, int(result[1])),
                    int(global_result[0]),
                    max(1, int(global_result[1])),
                )
            except (RedisError, TypeError, ValueError):
                # A local fallback keeps ingestion bounded if Redis is unavailable.
                pass
        session_count, session_ttl = await self._increment_memory(session_key)
        global_count, global_ttl = await self._increment_memory(global_key)
        return session_count, session_ttl, global_count, global_ttl


_rate_limiter = _AnonymousEventRateLimit()


@router.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def record_funnel_event(
    payload: FunnelEventInput,
    response: Response,
    db: Session = Depends(get_db),
):
    """Record one allowlisted event per anonymous session, event, and UTC day."""
    session_count, session_ttl, global_count, global_ttl = await _rate_limiter.check(
        payload.session_id
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-RateLimit-Limit"] = str(_rate_limiter.global_limit)
    response.headers["X-RateLimit-Remaining"] = str(
        max(0, _rate_limiter.global_limit - global_count)
    )
    response.headers["X-RateLimit-Reset"] = str(global_ttl)
    if (
        session_count > _rate_limiter.session_limit
        or global_count > _rate_limiter.global_limit
    ):
        response.headers["Retry-After"] = str(max(session_ttl, global_ttl))
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
