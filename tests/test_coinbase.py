import unittest
import json
import os
from unittest.mock import ANY, AsyncMock, patch

from fastapi import HTTPException

from routers.coinbase import (
    COINBASE_ACCOUNTS_PATH,
    COINBASE_KEY_PERMISSIONS_PATH,
    COINBASE_PORTFOLIOS_PATH,
    _build_auth_header,
    _build_coinbase_jwt,
    _fetch_coinbase_accounts,
    _fetch_coinbase_default_portfolio_breakdowns,
    _fetch_coinbase_portfolio_breakdowns,
    _next_page_params,
    _render_coinbase_csv,
    _validate_view_only_credentials,
    CoinbaseCapsuleRequest,
    create_coinbase_capsule,
    get_coinbase_balance,
)
from coinbase_capsule import (
    decrypt_coinbase_credentials,
    encrypt_coinbase_credentials,
)


class CoinbaseAuthHeaderTest(unittest.TestCase):
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
        self.assertEqual(
            payload["uri"], f"GET api.coinbase.com{COINBASE_ACCOUNTS_PATH}"
        )
        self.assertEqual(encode.call_args.kwargs["algorithm"], "ES256")

    def test_builds_auth_header_from_api_key_only(self):
        with patch("routers.coinbase._build_coinbase_jwt", return_value="signed"):
            result = _build_auth_header("key-name", "private-key")

        self.assertEqual(result["Authorization"], "Bearer signed")


class CoinbaseCapsuleCryptoTest(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(
            os.environ,
            {
                "COINBASE_CAPSULE_ACTIVE_KEY_ID": "v2",
                "COINBASE_CAPSULE_KEYS_JSON": json.dumps(
                    {
                        "v1": "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA",
                        "v2": "MTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTE",
                    }
                ),
            },
            clear=False,
        )
        self.environment.start()

    def tearDown(self):
        self.environment.stop()

    def test_round_trip_has_no_expiry_and_uses_active_version(self):
        capsule = encrypt_coinbase_credentials("key-name", "private-key")
        credentials = decrypt_coinbase_credentials(capsule)

        self.assertTrue(capsule.startswith("dhc1.v2."))
        self.assertEqual(credentials.key_name, "key-name")
        self.assertEqual(credentials.key_secret, "private-key")

    def test_encryption_uses_random_nonce(self):
        first = encrypt_coinbase_credentials("key-name", "private-key")
        second = encrypt_coinbase_credentials("key-name", "private-key")

        self.assertNotEqual(first, second)

    def test_old_key_remains_decryptable_after_rotation(self):
        with patch.dict(os.environ, {"COINBASE_CAPSULE_ACTIVE_KEY_ID": "v1"}):
            old_capsule = encrypt_coinbase_credentials("key-name", "private-key")

        self.assertEqual(
            decrypt_coinbase_credentials(old_capsule).key_name,
            "key-name",
        )

    def test_rejects_tampered_capsule_without_exposing_details(self):
        capsule = encrypt_coinbase_credentials("key-name", "private-key")
        tampered = f"{capsule[:-1]}{'A' if capsule[-1] != 'A' else 'B'}"

        with self.assertRaises(HTTPException) as context:
            decrypt_coinbase_credentials(tampered)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "Invalid Coinbase access key")

    def test_rejects_missing_server_keyring(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(HTTPException) as context:
                encrypt_coinbase_credentials("key-name", "private-key")

        self.assertEqual(context.exception.status_code, 503)


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

        with patch(
            "routers.coinbase._build_auth_header",
            return_value={"Authorization": "Bearer token"},
        ):
            result = await _fetch_coinbase_portfolio_breakdowns(
                client, "key-name", "private-key"
            )

        self.assertEqual(result[0]["portfolio"]["name"], "Perpetuals")
        self.assertEqual(
            client.requests[0]["url"].endswith(COINBASE_PORTFOLIOS_PATH), True
        )
        self.assertEqual(
            client.requests[0]["params"],
            {"portfolio_type": "INTX"},
        )
        self.assertEqual(
            client.requests[1]["url"].endswith(
                f"{COINBASE_PORTFOLIOS_PATH}/portfolio-1"
            ),
            True,
        )
        self.assertEqual(client.requests[1]["params"], {"currency": "USD"})

    async def test_skips_default_portfolio_without_key_access(self):
        client = FakeCoinbaseClient(
            [
                FakeCoinbaseResponse(
                    {
                        "portfolios": [
                            {
                                "uuid": "default-portfolio",
                                "name": "Default",
                                "deleted": False,
                            }
                        ]
                    }
                ),
                FakeCoinbaseResponse(
                    {
                        "error": "PERMISSION_DENIED",
                        "message": "User does not have access to portfolio",
                    },
                    status_code=403,
                ),
            ]
        )

        with patch(
            "routers.coinbase._build_auth_header",
            return_value={"Authorization": "Bearer token"},
        ):
            result = await _fetch_coinbase_default_portfolio_breakdowns(
                client, "key-name", "private-key"
            )

        self.assertEqual(result, [])
        self.assertEqual(client.requests[0]["params"], {"portfolio_type": "DEFAULT"})

    async def test_skips_intx_portfolios_without_key_access(self):
        client = FakeCoinbaseClient(
            [
                FakeCoinbaseResponse(
                    {
                        "error": "PERMISSION_DENIED",
                        "message": "User does not have access to portfolio",
                    },
                    status_code=403,
                )
            ]
        )

        with patch(
            "routers.coinbase._build_auth_header",
            return_value={"Authorization": "Bearer token"},
        ):
            result = await _fetch_coinbase_portfolio_breakdowns(
                client, "main-key", "main-secret"
            )

        self.assertEqual(result, [])
        self.assertEqual(client.requests[0]["params"], {"portfolio_type": "INTX"})


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

        self.assertIn(
            "account,btc-account,BTC Wallet,BTC,Bitcoin,crypto,1.2,BTC", content
        )
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

        self.assertIn(
            "portfolio_balance,portfolio-1:total_cash_equivalent_balance", content
        )
        self.assertIn(
            "spot_position,usdc-account,USDC,USDC,,cash,80253.341573,USDC", content
        )
        self.assertIn(
            "perp_position,btc-perp,BTC PERP,BTC,BTC-PERP,perp,0.1751,BTC", content
        )
        self.assertIn(",LONG,62563.3,64178.2,283.90,1422.3,10,", content)

    def test_renders_stable_total_across_default_and_intx_portfolios(self):
        content = _render_coinbase_csv(
            [],
            [
                {
                    "portfolio": {
                        "uuid": "default-portfolio",
                        "name": "Primary",
                        "type": "DEFAULT",
                    },
                    "portfolio_balances": {
                        "total_balance": {"value": "3590.355915", "currency": "USD"}
                    },
                },
                {
                    "portfolio": {
                        "uuid": "intx-portfolio",
                        "name": "Perpetuals",
                        "type": "INTX",
                    },
                    "portfolio_balances": {
                        "total_balance": {"value": "6.144085", "currency": "USD"}
                    },
                },
            ],
            include_zero=False,
        )

        self.assertIn(
            "portfolio_total,coinbase:total_balance,total_balance,USD,US Dollar,fiat,3596.500000,USD,3596.500000,USD",
            content,
        )

    def test_does_not_render_partial_total_without_default_portfolio(self):
        content = _render_coinbase_csv(
            [],
            [
                {
                    "portfolio": {
                        "uuid": "intx-portfolio",
                        "name": "Perpetuals",
                        "type": "INTX",
                    },
                    "portfolio_balances": {
                        "total_balance": {"value": "6.144085", "currency": "USD"}
                    },
                }
            ],
            include_zero=False,
        )

        self.assertNotIn("coinbase:total_balance", content)


class CoinbaseEndpointTest(unittest.IsolatedAsyncioTestCase):
    async def test_capsule_is_created_only_for_view_only_key(self):
        permissions = {
            "can_view": True,
            "can_trade": False,
            "can_transfer": False,
            "can_receive": False,
        }
        with (
            patch(
                "routers.coinbase._validate_view_only_credentials",
                new_callable=AsyncMock,
                return_value=permissions,
            ) as validate,
            patch(
                "routers.coinbase.encrypt_coinbase_credentials",
                return_value="dhc1.v1.encrypted",
            ) as encrypt,
        ):
            response = await create_coinbase_capsule(
                CoinbaseCapsuleRequest(
                    key_name="organizations/org/apiKeys/key",
                    key_secret="-----BEGIN EC PRIVATE KEY-----\\nsecret\\n-----END EC PRIVATE KEY-----",
                )
            )

        validate.assert_awaited_once()
        encrypt.assert_called_once()
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertNotIn("private", response.body.decode().lower())
        self.assertEqual(json.loads(response.body)["capsule"], "dhc1.v1.encrypted")

    async def test_rejects_key_with_dangerous_permissions(self):
        client = FakeCoinbaseClient(
            [
                FakeCoinbaseResponse(
                    {
                        "can_view": True,
                        "can_trade": True,
                        "can_transfer": False,
                        "can_receive": False,
                    }
                )
            ]
        )

        with patch("routers.coinbase._build_auth_header", return_value={}):
            with self.assertRaises(HTTPException) as context:
                await _validate_view_only_credentials(
                    client,
                    "key-name",
                    "private-key",
                )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("View-only", context.exception.detail)
        self.assertTrue(
            client.requests[0]["url"].endswith(COINBASE_KEY_PERMISSIONS_PATH)
        )

    async def test_rejects_key_without_view_permission(self):
        client = FakeCoinbaseClient(
            [
                FakeCoinbaseResponse(
                    {
                        "can_view": False,
                        "can_trade": False,
                        "can_transfer": False,
                        "can_receive": False,
                    }
                )
            ]
        )

        with patch("routers.coinbase._build_auth_header", return_value={}):
            with self.assertRaises(HTTPException) as context:
                await _validate_view_only_credentials(
                    client,
                    "key-name",
                    "private-key",
                )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("View permission", context.exception.detail)

    async def test_returns_csv_response(self):
        accounts = [
            {
                "id": "btc-account",
                "currency": {"code": "BTC"},
                "balance": {"amount": "1", "currency": "BTC"},
            }
        ]

        with (
            patch("routers.coinbase.decrypt_coinbase_credentials") as decrypt,
            patch("routers.coinbase._build_auth_header") as build_auth,
            patch(
                "routers.coinbase._fetch_coinbase_accounts", new_callable=AsyncMock
            ) as fetch,
            patch(
                "routers.coinbase._fetch_coinbase_portfolio_breakdowns",
                new_callable=AsyncMock,
            ) as fetch_portfolios,
            patch(
                "routers.coinbase._fetch_coinbase_default_portfolio_breakdowns",
                new_callable=AsyncMock,
            ) as fetch_default_portfolios,
        ):
            decrypt.return_value.key_name = "key-name"
            decrypt.return_value.key_secret = "private-key"
            build_auth.return_value = {"Authorization": "Bearer token"}
            fetch.return_value = accounts
            fetch_portfolios.return_value = []
            fetch_default_portfolios.return_value = []
            response = await get_coinbase_balance(
                capsule="dhc1.v1.encrypted",
                intx_capsule=None,
                include_zero=False,
                include_portfolios=True,
            )

        decrypt.assert_called_once_with("dhc1.v1.encrypted")
        build_auth.assert_called_once_with("key-name", "private-key")
        fetch.assert_awaited_once()
        fetch_default_portfolios.assert_awaited_once()
        fetch_portfolios.assert_awaited_once()
        self.assertEqual(response.media_type, "text/csv")
        self.assertIn("btc-account", response.body.decode())

    async def test_uses_a_separate_intx_capsule_for_perpetuals(self):
        with (
            patch("routers.coinbase.decrypt_coinbase_credentials") as decrypt,
            patch("routers.coinbase._build_auth_header") as build_auth,
            patch(
                "routers.coinbase._fetch_coinbase_accounts",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "routers.coinbase._fetch_coinbase_portfolio_breakdowns",
                new_callable=AsyncMock,
                return_value=[],
            ) as fetch_intx,
            patch(
                "routers.coinbase._fetch_coinbase_default_portfolio_breakdowns",
                new_callable=AsyncMock,
                return_value=[],
            ) as fetch_default,
        ):
            main_credentials = type(
                "Credentials",
                (),
                {"key_name": "main-key", "key_secret": "main-secret"},
            )()
            intx_credentials = type(
                "Credentials",
                (),
                {"key_name": "intx-key", "key_secret": "intx-secret"},
            )()
            decrypt.side_effect = [main_credentials, intx_credentials]
            build_auth.return_value = {"Authorization": "Bearer main"}

            await get_coinbase_balance(
                capsule="main-capsule",
                intx_capsule="intx-capsule",
                include_zero=False,
                include_portfolios=True,
            )

        self.assertEqual(
            [call.args[0] for call in decrypt.call_args_list],
            ["main-capsule", "intx-capsule"],
        )
        fetch_default.assert_awaited_once_with(
            ANY,
            "main-key",
            "main-secret",
        )
        fetch_intx.assert_awaited_once_with(
            ANY,
            "intx-key",
            "intx-secret",
        )
