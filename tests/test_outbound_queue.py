import json
import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import httpx

import outbound_queue as queue_module
from outbound_queue import (
    OutboundRequestQueue,
    ProviderPolicy,
    QueuedAsyncHTTPTransport,
    _request_cost,
    outbound_queue,
)


class OutboundRequestQueueTest(unittest.IsolatedAsyncioTestCase):
    def test_each_known_host_has_its_own_provider_policy(self):
        expected = {
            "pro-api.coinmarketcap.com": "coinmarketcap",
            "api.coinbase.com": "coinbase",
            "api.hyperliquid.xyz": "hyperliquid",
            "api.prod.paradex.trade": "paradex",
            "api.v3.aave.com": "aave_v3",
            "api.v4.aave.com": "aave_v4",
            "api.mainnet-beta.solana.com": "solana_rpc",
            "solana-rpc.publicnode.com": "solana_rpc",
            "ethereum-rpc.publicnode.com": "ethereum_rpc",
            "api.stakedao.org": "stakedao",
            "data-api.polymarket.com": "polymarket",
        }
        for host, provider in expected.items():
            with self.subTest(host=host):
                self.assertEqual(
                    outbound_queue.policy_for_host(host).name,
                    provider,
                )

    def test_hyperliquid_request_weights_are_applied(self):
        request = httpx.Request("POST", "https://api.hyperliquid.xyz/info")
        self.assertEqual(
            _request_cost("hyperliquid", request, b'{"type":"userRole"}'),
            60,
        )
        self.assertEqual(
            _request_cost(
                "hyperliquid",
                request,
                b'{"type":"clearinghouseState"}',
            ),
            2,
        )
        self.assertEqual(
            _request_cost("hyperliquid", request, b'{"type":"subAccounts"}'),
            20,
        )

    async def test_local_fallback_reserves_ordered_slots(self):
        queue = OutboundRequestQueue()
        policy = ProviderPolicy("test", ("test.example",), 2, 1, 1)
        queue._local_locks[policy.name] = queue_module.asyncio.Lock()

        with patch("outbound_queue.get_redis_client", return_value=None):
            first = await queue._reserve_local(policy, 1)
            second = await queue._reserve_local(policy, 1)

        self.assertEqual(first, 0)
        self.assertGreater(second, 0.45)

    async def test_status_reports_waiting_and_in_flight_requests(self):
        queue = OutboundRequestQueue()
        policy = queue.policy_for_host("mainnet.zklighter.elliot.ai")
        release = queue_module.asyncio.Event()
        started = 0
        both_started = queue_module.asyncio.Event()

        async def worker():
            nonlocal started
            async with queue.slot(policy, 1):
                started += 1
                if started == policy.concurrency:
                    both_started.set()
                await release.wait()

        with (
            patch("outbound_queue.get_redis_client", return_value=None),
            patch.object(queue, "_reserve_redis", AsyncMock(return_value=0)),
        ):
            active_tasks = [
                queue_module.asyncio.create_task(worker())
                for _ in range(policy.concurrency)
            ]
            await queue_module.asyncio.wait_for(both_started.wait(), timeout=1)
            waiting_task = queue_module.asyncio.create_task(worker())
            for _ in range(20):
                if queue._local_waiting.get(policy.name) == 1:
                    break
                await queue_module.asyncio.sleep(0)

            status = await queue.status(include_activity=True)
            provider = next(
                item for item in status["providers"] if item["provider"] == policy.name
            )
            self.assertEqual(provider["in_flight"], policy.concurrency)
            self.assertEqual(provider["waiting"], 1)
            self.assertEqual(provider["utilization_percent"], 100.0)

            release.set()
            await queue_module.asyncio.gather(*active_tasks, waiting_task)
            final_status = await queue.status(include_activity=True)

        final_provider = next(
            item
            for item in final_status["providers"]
            if item["provider"] == policy.name
        )
        self.assertEqual(final_provider["in_flight"], 0)
        self.assertEqual(final_provider["waiting"], 0)


class QueuedTransportTest(unittest.IsolatedAsyncioTestCase):
    async def test_429_is_delayed_and_retried(self):
        calls = 0

        async def handler(request: httpx.Request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(429, headers={"Retry-After": "0.01"})
            return httpx.Response(200, json={"ok": True})

        transport = QueuedAsyncHTTPTransport(transport=httpx.MockTransport(handler))

        @asynccontextmanager
        async def immediate_slot(policy, cost):
            yield

        with (
            patch.object(outbound_queue, "slot", immediate_slot),
            patch.object(outbound_queue, "cooldown", AsyncMock()) as cooldown,
        ):
            async with httpx.AsyncClient(transport=transport) as client:
                response = await client.post(
                    "https://api.coinbase.com/test",
                    content=json.dumps({"test": True}),
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(calls, 2)
        cooldown.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
