import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from routers.coinbase import (
    _build_auth_header,
    _fetch_coinbase_accounts,
    _render_coinbase_csv,
    get_coinbase_balance,
)


class CoinbaseAuthHeaderTest(unittest.TestCase):
    def test_adds_bearer_prefix(self):
        self.assertEqual(
            _build_auth_header("abc")["Authorization"],
            "Bearer abc",
        )

    def test_keeps_existing_bearer_prefix(self):
        self.assertEqual(
            _build_auth_header("Bearer abc")["Authorization"],
            "Bearer abc",
        )

    def test_rejects_empty_token(self):
        with self.assertRaises(HTTPException) as context:
            _build_auth_header(" ")

        self.assertEqual(context.exception.status_code, 400)


class FakeCoinbaseResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def json(self):
        return self._payload


class FakeCoinbaseClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    async def get(self, url, headers, params):
        self.requests.append({"url": url, "headers": headers, "params": params})
        return self.responses.pop(0)


class CoinbaseFetchAccountsTest(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_all_pages(self):
        client = FakeCoinbaseClient(
            [
                FakeCoinbaseResponse(
                    {
                        "has_next": True,
                        "cursor": "cursor-1",
                        "accounts": [{"uuid": "a"}],
                    }
                ),
                FakeCoinbaseResponse(
                    {
                        "has_next": False,
                        "accounts": [{"uuid": "b"}],
                    }
                ),
            ]
        )

        result = await _fetch_coinbase_accounts(client, "token")

        self.assertEqual(result, [{"uuid": "a"}, {"uuid": "b"}])
        self.assertEqual(client.requests[0]["params"], {"limit": "250"})
        self.assertEqual(
            client.requests[1]["params"], {"limit": "250", "cursor": "cursor-1"}
        )
        self.assertEqual(
            client.requests[0]["headers"]["Authorization"], "Bearer token"
        )

    async def test_rejects_invalid_accounts_shape(self):
        client = FakeCoinbaseClient(
            [FakeCoinbaseResponse({"has_next": False, "accounts": {}})]
        )

        with self.assertRaises(HTTPException) as context:
            await _fetch_coinbase_accounts(client, "token")

        self.assertEqual(context.exception.status_code, 502)


class CoinbaseCsvTest(unittest.TestCase):
    def test_renders_positive_balances_by_default(self):
        content = _render_coinbase_csv(
            [
                {
                    "uuid": "btc-account",
                    "name": "BTC Wallet",
                    "currency": "BTC",
                    "available_balance": {"value": "1.2", "currency": "BTC"},
                    "hold": {"value": "0.3", "currency": "BTC"},
                    "type": "CRYPTO",
                    "active": True,
                    "ready": True,
                    "default": False,
                },
                {
                    "uuid": "zero-account",
                    "name": "ZERO Wallet",
                    "currency": "ZERO",
                    "available_balance": {"value": "0", "currency": "ZERO"},
                    "hold": {"value": "0", "currency": "ZERO"},
                },
            ],
            include_zero=False,
        )

        self.assertIn("btc-account,BTC Wallet,BTC,1.2,0.3,1.5", content)
        self.assertNotIn("zero-account", content)

    def test_can_include_zero_balances(self):
        content = _render_coinbase_csv(
            [
                {
                    "uuid": "zero-account",
                    "currency": "ZERO",
                    "available_balance": {"value": "0", "currency": "ZERO"},
                    "hold": {"value": "0", "currency": "ZERO"},
                },
            ],
            include_zero=True,
        )

        self.assertIn("zero-account,,ZERO,0,0,0", content)


class CoinbaseEndpointTest(unittest.IsolatedAsyncioTestCase):
    async def test_returns_csv_response(self):
        accounts = [
            {
                "uuid": "btc-account",
                "currency": "BTC",
                "available_balance": {"value": "1", "currency": "BTC"},
                "hold": {"value": "0", "currency": "BTC"},
            }
        ]

        with patch(
            "routers.coinbase._fetch_coinbase_accounts", new_callable=AsyncMock
        ) as fetch:
            fetch.return_value = accounts
            response = await get_coinbase_balance("token")

        fetch.assert_awaited_once()
        self.assertEqual(response.media_type, "text/csv")
        self.assertIn("btc-account", response.body.decode())
