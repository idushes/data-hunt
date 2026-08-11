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
