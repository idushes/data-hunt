from fastapi import APIRouter, Depends, HTTPException, status
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from config import REDIS_URL
from database import get_db
from outbound_queue import outbound_queue
from redis_client import redis_ping

router = APIRouter(prefix="/health", tags=["health"])


@router.get(
    "/liveness",
    summary="Liveness Probe",
    description="K8s liveness probe. Returns 200 OK if the service is running.",
)
def liveness():
    return {"status": "ok"}


@router.get(
    "/readiness",
    summary="Readiness Probe",
    description="K8s readiness probe. Checks database connectivity. Returns 200 OK if DB is reachable, 503 otherwise.",
)
async def readiness(db: Session = Depends(get_db)):
    try:
        # Simple query to check DB connection
        db.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection failed",
        ) from exc
    if REDIS_URL:
        try:
            redis_ready = await redis_ping()
        except RedisError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Redis connection failed",
            ) from exc
        if not redis_ready:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Redis connection failed",
            )
    return {"status": "ready", "redis": "ready" if REDIS_URL else "disabled"}


@router.get(
    "/queues",
    summary="Outbound API queue status",
    description="Shows per-provider limits, next-slot delay, and active cooldowns.",
)
async def queues():
    return await outbound_queue.status()
