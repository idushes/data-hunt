import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from routers.coinbase import (
    COINBASE_ACCOUNTS_PATH,
    COINBASE_PORTFOLIOS_PATH,
    _build_auth_header,
    _build_coinbase_jwt,
    _fetch_coinbase_accounts,
    _fetch_coinbase_portfolio_breakdowns,
    _next_page_params,
    _render_coinbase_csv,
    get_coinbase_balance,
)


class CoinbaseAuthHeaderTest(unittest.TestCase):
    def test_adds_bearer_prefix_to_ready_token(self):
        self.assertEqual(
            _build_auth_header("abc", None, None)["Authorization"],
            "Bearer abc",
        )

    def test_keeps_existing_bearer_prefix(self):
        self.assertEqual(
            _build_auth_header("Bearer abc", None, None)["Authorization"],
            "Bearer abc",
        )

    def test_rejects_empty_token(self):
        with self.assertRaises(HTTPException) as context:
            _build_auth_header(" ", None, None)

        self.assertEqual(context.exception.status_code, 400)

    def test_builds_jwt_from_api_key(self):
        with patch("routers.coinbase.jwt.encode") as encode:
            encode.return_value = "signed-token"

            result = _build_coinbase_jwt(
                "organizations/org/apiKeys/key",
                "-----BEGIN EC PRIVATE KEY-----\\nsecret\\n-----END EC PRIVATE KEY-----",
            )

        self.assertEqual(result, "signed-token")
        payload = encode.call_args.args[0]
        self.assertEqual(payload["sub"], "organizations/org/apiKeys/key")
        self.assertEqual(payload["iss"], "cdp")
        self.assertEqual(payload["uri"], f"GET api.coinbase.com{COINBASE_ACCOUNTS_PATH}")
        self.assertEqual(encode.call_args.kwargs["algorithm"], "ES256")

    def test_rejects_missing_credentials(self):
        with self.assertRaises(HTTPException) as context:
            _build_auth_header(None, None, None)

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


class CoinbasePaginationTest(unittest.TestCase):
    def test_extracts_next_page_params(self):
        self.assertEqual(
            _next_page_params("/v2/accounts?starting_after=abc&limit=100"),
            {"starting_after": "abc", "limit": "100"},
        )

    def test_rejects_unexpected_next_page_path(self):
        with self.assertRaises(HTTPException) as context:
            _next_page_params("/v2/users?starting_after=abc")

        self.assertEqual(context.exception.status_code, 502)


class CoinbaseFetchAccountsTest(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_all_pages(self):
        client = FakeCoinbaseClient(
            [
                FakeCoinbaseResponse(
                    {
                        "pagination": {
                            "next_uri": "/v2/accounts?starting_after=cursor-1&limit=100"
                        },
                        "data": [{"id": "a"}],
                    }
                ),
                FakeCoinbaseResponse(
                    {
                        "pagination": {"next_uri": None},
                        "data": [{"id": "b"}],
                    }
                ),
            ]
        )

        result = await _fetch_coinbase_accounts(
            client, {"Authorization": "Bearer token"}
        )

        self.assertEqual(result, [{"id": "a"}, {"id": "b"}])
        self.assertEqual(client.requests[0]["params"], {"limit": "100"})
        self.assertEqual(
            client.requests[1]["params"],
            {"starting_after": "cursor-1", "limit": "100"},
        )
        self.assertEqual(client.requests[0]["headers"]["Authorization"], "Bearer token")

    async def test_rejects_invalid_accounts_shape(self):
        client = FakeCoinbaseClient(
            [FakeCoinbaseResponse({"pagination": {}, "data": {}})]
        )

        with self.assertRaises(HTTPException) as context:
            await _fetch_coinbase_accounts(client, {"Authorization": "Bearer token"})

        self.assertEqual(context.exception.status_code, 502)


class CoinbasePortfolioBreakdownTest(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_intx_portfolio_breakdowns(self):
        client = FakeCoinbaseClient(
            [
                FakeCoinbaseResponse(
                    {
                        "portfolios": [
                            {
                                "uuid": "portfolio-1",
                                "name": "Perpetuals",
                                "deleted": False,
                            }
                        ]
                    }
                ),
                FakeCoinbaseResponse(
                    {
                        "breakdown": {
                            "portfolio": {
                                "uuid": "portfolio-1",
                                "name": "Perpetuals",
                            },
                            "spot_positions": [],
                            "perp_positions": [],
                        }
                    }
                ),
            ]
        )

        result = await _fetch_coinbase_portfolio_breakdowns(
            client, "Bearer token", None, None
        )

        self.assertEqual(result[0]["portfolio"]["name"], "Perpetuals")
        self.assertEqual(client.requests[0]["url"].endswith(COINBASE_PORTFOLIOS_PATH), True)
        self.assertEqual(
            client.requests[0]["params"],
            {"portfolio_type": "INTX"},
        )
        self.assertEqual(
            client.requests[1]["url"].endswith(f"{COINBASE_PORTFOLIOS_PATH}/portfolio-1"),
            True,
        )


class CoinbaseCsvTest(unittest.TestCase):
    def test_renders_positive_balances_by_default(self):
        content = _render_coinbase_csv(
            [
                {
                    "id": "btc-account",
                    "name": "BTC Wallet",
                    "currency": {
                        "code": "BTC",
                        "name": "Bitcoin",
                        "type": "crypto",
                    },
                    "balance": {"amount": "1.2", "currency": "BTC"},
                    "type": "wallet",
                    "primary": True,
                    "ready": True,
                },
                {
                    "id": "zero-account",
                    "name": "ZERO Wallet",
                    "currency": {
                        "code": "ZERO",
                        "name": "Zero",
                        "type": "crypto",
                    },
                    "balance": {"amount": "0", "currency": "ZERO"},
                },
            ],
            [],
            include_zero=False,
        )

        self.assertIn("account,btc-account,BTC Wallet,BTC,Bitcoin,crypto,1.2,BTC", content)
        self.assertNotIn("zero-account", content)

    def test_can_include_zero_balances(self):
        content = _render_coinbase_csv(
            [
                {
                    "id": "zero-account",
                    "currency": {"code": "ZERO"},
                    "balance": {"amount": "0", "currency": "ZERO"},
                },
            ],
            [],
            include_zero=True,
        )

        self.assertIn("account,zero-account,,ZERO,,,0,ZERO", content)

    def test_renders_portfolio_balances_spot_and_perp_positions(self):
        content = _render_coinbase_csv(
            [],
            [
                {
                    "portfolio": {
                        "uuid": "portfolio-1",
                        "name": "Perpetuals",
                    },
                    "portfolio_balances": {
                        "total_cash_equivalent_balance": {
                            "value": "80253.34",
                            "currency": "USD",
                        }
                    },
                    "spot_positions": [
                        {
                            "asset": "USDC",
                            "account_uuid": "usdc-account",
                            "total_balance_crypto": "80253.341573",
                            "total_balance_fiat": "80253.34",
                            "available_to_trade_crypto": "80253.341573",
                            "account_type": "ACCOUNT_TYPE_CRYPTO",
                            "is_cash": True,
                        }
                    ],
                    "perp_positions": [
                        {
                            "product_uuid": "btc-perp",
                            "product_id": "BTC-PERP",
                            "symbol": "BTC PERP",
                            "net_size": "0.1751",
                            "position_side": "LONG",
                            "position_notional": {
                                "rawCurrency": {
                                    "value": "11237.6",
                                    "currency": "USDC",
                                }
                            },
                            "vwap": {
                                "rawCurrency": {
                                    "value": "62563.3",
                                    "currency": "USDC",
                                }
                            },
                            "mark_price": {
                                "rawCurrency": {
                                    "value": "64178.2",
                                    "currency": "USDC",
                                }
                            },
                            "unrealized_pnl": {
                                "rawCurrency": {
                                    "value": "283.90",
                                    "currency": "USDC",
                                }
                            },
                            "im_contribution": "1422.3",
                            "leverage": "10",
                            "margin_type": "CROSS",
                        }
                    ],
                }
            ],
            include_zero=False,
        )

        self.assertIn("portfolio_balance,portfolio-1:total_cash_equivalent_balance", content)
        self.assertIn("spot_position,usdc-account,USDC,USDC,,cash,80253.341573,USDC", content)
        self.assertIn("perp_position,btc-perp,BTC PERP,BTC,BTC-PERP,perp,0.1751,BTC", content)
        self.assertIn(",LONG,62563.3,64178.2,283.90,1422.3,10,", content)


class CoinbaseEndpointTest(unittest.IsolatedAsyncioTestCase):
    async def test_returns_csv_response(self):
        accounts = [
            {
                "id": "btc-account",
                "currency": {"code": "BTC"},
                "balance": {"amount": "1", "currency": "BTC"},
            }
        ]

        with (
            patch("routers.coinbase._build_auth_header") as build_auth,
            patch(
                "routers.coinbase._fetch_coinbase_accounts", new_callable=AsyncMock
            ) as fetch,
            patch(
                "routers.coinbase._fetch_coinbase_portfolio_breakdowns",
                new_callable=AsyncMock,
            ) as fetch_portfolios,
        ):
            build_auth.return_value = {"Authorization": "Bearer token"}
            fetch.return_value = accounts
            fetch_portfolios.return_value = []
            response = await get_coinbase_balance("token")

        build_auth.assert_called_once()
        fetch.assert_awaited_once()
        fetch_portfolios.assert_awaited_once()
        self.assertEqual(response.media_type, "text/csv")
        self.assertIn("btc-account", response.body.decode())
