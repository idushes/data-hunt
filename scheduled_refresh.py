import asyncio
import hashlib
import json
import logging
import re
import time
from typing import Callable

import httpx
from fastapi import Request, Response
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask, BackgroundTasks
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from config import (
    SHEETS_REFRESH_DELAY_SECONDS,
    SHEETS_REFRESH_ENABLED,
    SHEETS_REFRESH_POLL_SECONDS,
)
from csv_cache import CACHE_FORCE_REFRESH_HEADER
from database import SessionLocal
from models import ValueResource
from redis_client import get_redis_client
from value_rate_limit import (
    DATA_ACCESS_INTERNAL_HEADER,
    DATA_ACCESS_INTERNAL_TOKEN,
)


logger = logging.getLogger(__name__)

REFRESH_QUEUE_KEY = "datahunt:sheets-refresh:v1"
REFRESH_RESOURCE_KEY_PREFIX = "datahunt:sheets-refresh:resource:v1:"
REFRESH_RESOURCE_TTL_SECONDS = 7 * 24 * 60 * 60
RESOURCE_ID_PATTERN = re.compile(r"^/v/([A-Za-z0-9_-]{12,22})$")
CLAIM_DUE_SCRIPT = """
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
if #items == 0 then
    return nil
end
if redis.call('ZREM', KEYS[1], items[1]) == 1 then
    return items[1]
end
return nil
"""


class ScheduledRefreshQueue:
    def __init__(
        self,
        *,
        enabled: bool = SHEETS_REFRESH_ENABLED,
        delay_seconds: int = SHEETS_REFRESH_DELAY_SECONDS,
        poll_seconds: float = SHEETS_REFRESH_POLL_SECONDS,
        redis_client: Redis | None = None,
        session_factory: Callable[[], Session] = SessionLocal,
    ):
        self.enabled = enabled
        self.delay_seconds = max(60, delay_seconds)
        self.poll_seconds = max(0.1, poll_seconds)
        self._redis_client = redis_client
        self._session_factory = session_factory
        self._app = None
        self._worker: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def _redis(self) -> Redis | None:
        return self._redis_client or get_redis_client()

    @staticmethod
    def _refresh_fingerprint(source: str, parameters: dict[str, str]) -> str:
        canonical = json.dumps(
            {"source": source, "parameters": parameters},
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()

    def _load_resource(
        self, resource_id: str
    ) -> tuple[str, dict[str, str]] | None:
        with self._session_factory() as db:
            resource = db.get(ValueResource, resource_id)
            if resource is None:
                return None
            parameters = resource.parameters or {}
            return resource.source, dict(sorted(parameters.items()))

    @staticmethod
    def _mapping_key(fingerprint: str) -> str:
        return f"{REFRESH_RESOURCE_KEY_PREFIX}{fingerprint}"

    async def schedule(self, resource_id: str) -> bool:
        if not self.enabled:
            return False
        client = self._redis()
        if client is None:
            return False
        resource = self._load_resource(resource_id)
        if resource is None:
            return False
        source, parameters = resource

        from routers.value import RESOURCE_CREDENTIAL_PARAMS

        if RESOURCE_CREDENTIAL_PARAMS.get(source):
            return False

        fingerprint = self._refresh_fingerprint(source, parameters)
        due_at = time.time() + self.delay_seconds
        try:
            async with client.pipeline(transaction=True) as pipeline:
                pipeline.set(
                    self._mapping_key(fingerprint),
                    resource_id,
                    ex=REFRESH_RESOURCE_TTL_SECONDS,
                )
                pipeline.zadd(REFRESH_QUEUE_KEY, {fingerprint: due_at}, nx=True)
                await pipeline.execute()
            return True
        except RedisError as exc:
            logger.warning("Could not schedule Sheets cache refresh: %s", exc)
            return False

    async def _claim_due(self) -> str | None:
        client = self._redis()
        if client is None:
            return None
        claimed = await client.eval(
            CLAIM_DUE_SCRIPT,
            1,
            REFRESH_QUEUE_KEY,
            str(time.time()),
        )
        if claimed is None:
            return None
        return claimed.decode() if isinstance(claimed, bytes) else str(claimed)

    async def _resource_id_for_fingerprint(
        self, fingerprint: str
    ) -> str | None:
        client = self._redis()
        if client is None:
            return None
        value = await client.get(self._mapping_key(fingerprint))
        if value is None:
            return None
        return value.decode() if isinstance(value, bytes) else str(value)

    async def _refresh_resource(self, resource_id: str) -> bool:
        if self._app is None:
            return False
        resource = self._load_resource(resource_id)
        if resource is None:
            return False
        source, parameters = resource

        from routers.value import (
            RESOURCE_CREDENTIAL_PARAMS,
            _resource_source_path,
        )

        if RESOURCE_CREDENTIAL_PARAMS.get(source):
            return False
        path = _resource_source_path(source)
        if path is None:
            return False

        transport = httpx.ASGITransport(app=self._app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://data-hunt.internal",
            timeout=180.0,
        ) as client:
            response = await client.get(
                path,
                params=parameters,
                headers={
                    DATA_ACCESS_INTERNAL_HEADER: DATA_ACCESS_INTERNAL_TOKEN,
                    CACHE_FORCE_REFRESH_HEADER: "1",
                },
            )
        if response.status_code != 200:
            logger.warning(
                "Scheduled Sheets refresh failed for source=%s status=%s",
                source,
                response.status_code,
            )
            return False
        logger.info("Scheduled Sheets refresh completed for source=%s", source)
        return True

    async def run_due_once(self) -> bool:
        try:
            fingerprint = await self._claim_due()
            if fingerprint is None:
                return False
            resource_id = await self._resource_id_for_fingerprint(fingerprint)
            if resource_id is None:
                return True
            await self._refresh_resource(resource_id)
            return True
        except RedisError as exc:
            logger.warning("Scheduled Sheets refresh queue unavailable: %s", exc)
            return False
        except Exception:
            logger.exception("Scheduled Sheets refresh failed")
            return True

    async def _run(self) -> None:
        while not self._stop.is_set():
            handled = await self.run_due_once()
            if handled:
                continue
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self.poll_seconds,
                )
            except asyncio.TimeoutError:
                continue

    async def start(self, app) -> None:
        self._app = app
        if not self.enabled or self._redis() is None or self._worker is not None:
            return
        self._stop.clear()
        self._worker = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._worker is not None:
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)
        self._worker = None
        self._app = None

    async def status(self) -> dict[str, object]:
        client = self._redis()
        if not self.enabled or client is None:
            return {"enabled": self.enabled, "queued": 0, "due": 0}
        try:
            queued = int(await client.zcard(REFRESH_QUEUE_KEY))
            due = int(await client.zcount(REFRESH_QUEUE_KEY, "-inf", time.time()))
            return {"enabled": True, "queued": queued, "due": due}
        except RedisError:
            return {"enabled": True, "queued": 0, "due": 0, "redis": False}


scheduled_refresh = ScheduledRefreshQueue()


class ScheduledRefreshMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        response = await call_next(request)
        match = RESOURCE_ID_PATTERN.fullmatch(request.url.path)
        is_internal = (
            request.headers.get(DATA_ACCESS_INTERNAL_HEADER)
            == DATA_ACCESS_INTERNAL_TOKEN
        )
        query_names = {name for name, _ in request.query_params.multi_items()}
        if (
            request.method.upper() == "GET"
            and response.status_code < 400
            and match is not None
            and not is_internal
            and query_names <= {"auth_token"}
        ):
            task = BackgroundTask(scheduled_refresh.schedule, match.group(1))
            if response.background is None:
                response.background = task
            elif isinstance(response.background, BackgroundTasks):
                response.background.tasks.append(task)
            else:
                response.background = BackgroundTasks([response.background, task])
        return response
