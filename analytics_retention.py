"""Retention worker for anonymous product analytics."""

import asyncio
import logging
import time
from typing import Callable

from sqlalchemy.orm import Session

from database import SessionLocal
from models import AuthFunnelEvent


logger = logging.getLogger(__name__)

AUTH_FUNNEL_RETENTION_DAYS = 30
AUTH_FUNNEL_RETENTION_POLL_SECONDS = 6 * 60 * 60


class AuthFunnelRetention:
    """Delete anonymous event rows outside the longest supported admin period."""

    def __init__(
        self,
        *,
        retention_days: int = AUTH_FUNNEL_RETENTION_DAYS,
        poll_seconds: float = AUTH_FUNNEL_RETENTION_POLL_SECONDS,
        session_factory: Callable[[], Session] = SessionLocal,
    ):
        self.retention_days = max(1, retention_days)
        self.poll_seconds = max(1.0, poll_seconds)
        self._session_factory = session_factory
        self._worker: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def delete_expired(self, current_day: int | None = None) -> int:
        if current_day is None:
            current_day = int(time.time()) // 86400
        first_retained_day = current_day - self.retention_days + 1
        with self._session_factory() as db:
            deleted = (
                db.query(AuthFunnelEvent)
                .filter(AuthFunnelEvent.day < first_retained_day)
                .delete(synchronize_session=False)
            )
            db.commit()
        return int(deleted)

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                deleted = self.delete_expired()
                if deleted:
                    logger.info("Deleted %s expired anonymous funnel events", deleted)
            except Exception:
                logger.exception("Anonymous funnel retention cleanup failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_seconds)
            except asyncio.TimeoutError:
                continue

    async def start(self) -> None:
        if self._worker is not None:
            return
        self._stop.clear()
        self._worker = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._worker is not None:
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)
        self._worker = None


auth_funnel_retention = AuthFunnelRetention()
