import asyncio
import json
import logging
import math
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from email.utils import parsedate_to_datetime

import httpx
from redis.exceptions import RedisError

from config import (
    OUTBOUND_API_LIMITS_JSON,
    OUTBOUND_QUEUE_429_RETRIES,
    OUTBOUND_QUEUE_ENABLED,
    OUTBOUND_QUEUE_MAX_WAIT_SECONDS,
)
from redis_client import get_redis_client

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProviderPolicy:
    name: str
    hosts: tuple[str, ...]
    requests: int
    period_seconds: float
    concurrency: int

    @property
    def interval_ms(self) -> float:
        return self.period_seconds * 1000 / self.requests


# Published limits get headroom. Providers without public limits use deliberately
# conservative defaults and can be adjusted without code via OUTBOUND_API_LIMITS_JSON.
DEFAULT_POLICIES = (
    ProviderPolicy("coinmarketcap", ("pro-api.coinmarketcap.com",), 45, 60, 2),
    ProviderPolicy("coinbase", ("api.coinbase.com",), 8, 1, 4),
    ProviderPolicy("hyperliquid", ("api.hyperliquid.xyz",), 1080, 60, 4),
    ProviderPolicy("paradex", ("api.prod.paradex.trade",), 8, 1, 4),
    ProviderPolicy("lighter", ("mainnet.zklighter.elliot.ai",), 3, 1, 2),
    ProviderPolicy("fluid", ("api.fluid.io",), 3, 1, 2),
    ProviderPolicy("instadapp", ("api.instadapp.io",), 3, 1, 2),
    ProviderPolicy("fluid_lite", ("api.fluid-lite.instadapp.ai",), 3, 1, 2),
    ProviderPolicy("aave_v3", ("api.v3.aave.com",), 3, 1, 2),
    ProviderPolicy("aave_v4", ("api.v4.aave.com",), 3, 1, 2),
    ProviderPolicy("ethereum_rpc", ("ethereum-rpc.publicnode.com",), 4, 1, 4),
    ProviderPolicy("monad_rpc", ("rpc.monad.xyz",), 4, 1, 4),
    ProviderPolicy("solana_rpc", ("api.mainnet-beta.solana.com",), 30, 10, 6),
    ProviderPolicy("kamino", ("api.kamino.finance", "cdn.kamino.com"), 3, 1, 3),
    ProviderPolicy("kamino_rpc", ("helius-rpc.kamino.com",), 4, 1, 4),
    ProviderPolicy("gmtrade_rpc", ("rpc-1.gmtrade.xyz",), 3, 1, 3),
    ProviderPolicy("gmtrade_api", ("gmtrade-web-backend.gmtrade.xyz",), 3, 1, 3),
    ProviderPolicy("gmx_squid", ("gmx-solana-sqd.squids.live",), 3, 1, 3),
    ProviderPolicy("stakedao", ("api.stakedao.org",), 3, 1, 2),
    ProviderPolicy("blockscout", ("eth.blockscout.com",), 3, 1, 2),
    ProviderPolicy("curve", ("api.curve.finance",), 2, 1, 2),
    ProviderPolicy("morpho", ("api.morpho.org",), 3, 1, 2),
    ProviderPolicy("base_rpc", ("base-rpc.publicnode.com",), 4, 1, 4),
    ProviderPolicy(
        "arbitrum_rpc", ("arbitrum-one-rpc.publicnode.com",), 4, 1, 4
    ),
)
FALLBACK_POLICY = ProviderPolicy("other", (), 2, 1, 2)

RATE_SLOT_SCRIPT = """
local now_parts = redis.call('TIME')
local now_ms = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000)
local interval_ms = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])
local max_wait_ms = tonumber(ARGV[3])
local current = tonumber(redis.call('GET', KEYS[1])) or now_ms
local scheduled = math.max(now_ms, current)
local wait_ms = scheduled - now_ms
if wait_ms > max_wait_ms then
    return {-1, wait_ms}
end
local next_slot = scheduled + (interval_ms * cost)
local ttl_ms = math.max(1000, math.ceil(next_slot - now_ms + max_wait_ms))
redis.call('SET', KEYS[1], tostring(next_slot), 'PX', ttl_ms)
return {math.floor(wait_ms), math.floor(next_slot)}
"""

COOLDOWN_SCRIPT = """
local requested_ttl = tonumber(ARGV[1])
local current_ttl = redis.call('PTTL', KEYS[1])
if current_ttl < requested_ttl then
    redis.call('SET', KEYS[1], '1', 'PX', requested_ttl)
end
return math.max(current_ttl, requested_ttl)
"""


class OutboundQueueTimeout(httpx.PoolTimeout):
    pass


def _load_policies() -> tuple[ProviderPolicy, ...]:
    if not OUTBOUND_API_LIMITS_JSON.strip():
        return DEFAULT_POLICIES
    try:
        overrides = json.loads(OUTBOUND_API_LIMITS_JSON)
    except (TypeError, ValueError):
        logger.error("OUTBOUND_API_LIMITS_JSON is invalid; using defaults")
        return DEFAULT_POLICIES
    if not isinstance(overrides, dict):
        logger.error("OUTBOUND_API_LIMITS_JSON must be an object; using defaults")
        return DEFAULT_POLICIES

    policies = []
    for policy in DEFAULT_POLICIES:
        raw = overrides.get(policy.name)
        if not isinstance(raw, dict):
            policies.append(policy)
            continue
        try:
            policies.append(
                replace(
                    policy,
                    requests=max(1, int(raw.get("requests", policy.requests))),
                    period_seconds=max(
                        0.1,
                        float(raw.get("period_seconds", policy.period_seconds)),
                    ),
                    concurrency=max(1, int(raw.get("concurrency", policy.concurrency))),
                )
            )
        except (TypeError, ValueError):
            logger.error("Invalid queue override for %s; using default", policy.name)
            policies.append(policy)
    return tuple(policies)


def _retry_after_seconds(response: httpx.Response, attempt: int) -> float:
    value = response.headers.get("retry-after")
    if value:
        try:
            return max(0.1, min(float(value), 120.0))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                return max(0.1, min(retry_at.timestamp() - time.time(), 120.0))
            except (TypeError, ValueError):
                pass
    return min(2**attempt, 30.0)


def _request_cost(provider: str, request: httpx.Request, content: bytes) -> int:
    if provider != "hyperliquid" or request.url.path != "/info":
        return 1
    try:
        payload = json.loads(content)
    except (TypeError, ValueError):
        return 20
    request_type = payload.get("type") if isinstance(payload, dict) else None
    if request_type == "userRole":
        return 60
    if request_type in {
        "l2Book",
        "allMids",
        "clearinghouseState",
        "orderStatus",
        "spotClearinghouseState",
        "exchangeStatus",
    }:
        return 2
    return 20


class OutboundRequestQueue:
    def __init__(self):
        self.policies = _load_policies()
        self.by_host = {
            host: policy for policy in self.policies for host in policy.hosts
        }
        self._semaphores = {
            policy.name: asyncio.Semaphore(policy.concurrency)
            for policy in (*self.policies, FALLBACK_POLICY)
        }
        self._local_locks = {
            policy.name: asyncio.Lock() for policy in (*self.policies, FALLBACK_POLICY)
        }
        self._local_next_slot: dict[str, float] = {}
        self._local_cooldown: dict[str, float] = {}
        self._last_redis_warning = 0.0

    def policy_for_host(self, host: str | None) -> ProviderPolicy:
        return self.by_host.get((host or "").lower(), FALLBACK_POLICY)

    def _warn_redis(self, exc: Exception) -> None:
        now = time.monotonic()
        if now - self._last_redis_warning >= 30:
            logger.warning(
                "Redis request queue unavailable; using local limiter: %s", exc
            )
            self._last_redis_warning = now

    async def _reserve_redis(self, policy: ProviderPolicy, cost: int) -> float | None:
        client = get_redis_client()
        if client is None:
            return None
        try:
            cooldown_ms = await client.pttl(f"datahunt:queue:cooldown:{policy.name}")
            if cooldown_ms > 0:
                await asyncio.sleep(cooldown_ms / 1000)
            result = await client.eval(
                RATE_SLOT_SCRIPT,
                1,
                f"datahunt:queue:slot:{policy.name}",
                policy.interval_ms,
                cost,
                OUTBOUND_QUEUE_MAX_WAIT_SECONDS * 1000,
            )
            if int(result[0]) < 0:
                raise OutboundQueueTimeout(
                    f"{policy.name} queue wait would exceed "
                    f"{OUTBOUND_QUEUE_MAX_WAIT_SECONDS}s"
                )
            return int(result[0]) / 1000
        except OutboundQueueTimeout:
            raise
        except RedisError as exc:
            self._warn_redis(exc)
            return None

    async def _reserve_local(self, policy: ProviderPolicy, cost: int) -> float:
        async with self._local_locks[policy.name]:
            now = time.monotonic()
            cooldown = max(0.0, self._local_cooldown.get(policy.name, 0.0) - now)
            scheduled = max(
                now + cooldown,
                self._local_next_slot.get(policy.name, now),
            )
            wait = scheduled - now
            if wait > OUTBOUND_QUEUE_MAX_WAIT_SECONDS:
                raise OutboundQueueTimeout(
                    f"{policy.name} queue wait would exceed "
                    f"{OUTBOUND_QUEUE_MAX_WAIT_SECONDS}s"
                )
            self._local_next_slot[policy.name] = (
                scheduled + policy.interval_ms * cost / 1000
            )
            return wait

    async def _set_cooldown(self, policy: ProviderPolicy, seconds: float) -> None:
        ttl_ms = max(100, math.ceil(seconds * 1000))
        client = get_redis_client()
        if client is not None:
            try:
                await client.eval(
                    COOLDOWN_SCRIPT,
                    1,
                    f"datahunt:queue:cooldown:{policy.name}",
                    ttl_ms,
                )
                return
            except RedisError as exc:
                self._warn_redis(exc)
        self._local_cooldown[policy.name] = max(
            self._local_cooldown.get(policy.name, 0.0),
            time.monotonic() + seconds,
        )

    @asynccontextmanager
    async def slot(
        self,
        policy: ProviderPolicy,
        cost: int,
    ) -> AsyncIterator[None]:
        if not OUTBOUND_QUEUE_ENABLED:
            yield
            return
        wait = await self._reserve_redis(policy, cost)
        if wait is None:
            wait = await self._reserve_local(policy, cost)
        if wait > 0:
            await asyncio.sleep(wait)
        semaphore = self._semaphores[policy.name]
        async with semaphore:
            yield

    async def cooldown(self, policy: ProviderPolicy, seconds: float) -> None:
        await self._set_cooldown(policy, seconds)

    async def status(self) -> dict[str, object]:
        client = get_redis_client()
        now_ms = time.time() * 1000
        result = []
        for policy in (*self.policies, FALLBACK_POLICY):
            next_slot_delay_ms = 0
            cooldown_ms = 0
            if client is not None:
                try:
                    next_slot, cooldown_ms = await asyncio.gather(
                        client.get(f"datahunt:queue:slot:{policy.name}"),
                        client.pttl(f"datahunt:queue:cooldown:{policy.name}"),
                    )
                    if next_slot is not None:
                        next_slot_delay_ms = max(
                            0,
                            math.ceil(float(next_slot) - now_ms),
                        )
                except (RedisError, TypeError, ValueError) as exc:
                    self._warn_redis(exc)
            result.append(
                {
                    "provider": policy.name,
                    "hosts": policy.hosts,
                    "requests": policy.requests,
                    "period_seconds": policy.period_seconds,
                    "concurrency": policy.concurrency,
                    "next_slot_delay_ms": next_slot_delay_ms,
                    "cooldown_ms": max(0, int(cooldown_ms)),
                }
            )
        return {
            "enabled": OUTBOUND_QUEUE_ENABLED,
            "redis": client is not None,
            "max_wait_seconds": OUTBOUND_QUEUE_MAX_WAIT_SECONDS,
            "providers": result,
        }


outbound_queue = OutboundRequestQueue()


class QueuedAsyncHTTPTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        *,
        trust_env: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._transport = transport or httpx.AsyncHTTPTransport(
            trust_env=trust_env,
            retries=0,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        content = await request.aread()
        policy = outbound_queue.policy_for_host(request.url.host)
        cost = _request_cost(policy.name, request, content)

        for attempt in range(OUTBOUND_QUEUE_429_RETRIES + 1):
            queued_request = httpx.Request(
                method=request.method,
                url=request.url,
                headers=request.headers,
                content=content,
                extensions=request.extensions,
            )
            async with outbound_queue.slot(policy, cost):
                response = await self._transport.handle_async_request(queued_request)
            if response.status_code != 429:
                return response

            retry_after = _retry_after_seconds(response, attempt)
            await outbound_queue.cooldown(policy, retry_after)
            if attempt >= OUTBOUND_QUEUE_429_RETRIES:
                return response
            await response.aclose()

        raise RuntimeError("unreachable")

    async def aclose(self) -> None:
        await self._transport.aclose()


def queued_async_client(
    *,
    timeout: float | httpx.Timeout = 20.0,
    trust_env: bool = True,
    **kwargs,
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=timeout,
        transport=QueuedAsyncHTTPTransport(trust_env=trust_env),
        **kwargs,
    )
