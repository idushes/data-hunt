import base64
import csv
import io
import json
import os
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from bybit_capsule import decrypt_bybit_credentials, encrypt_bybit_credentials
from routers.bybit import (
    _api_base_url,
    _fetch_funding_balances,
    _fetch_spot_usd_prices,
    _render_bybit_csv,
    _signed_headers,
    create_bybit_capsule,
    get_bybit_account_csv,
    BybitCapsuleRequest,
)


class BybitCapsuleTest(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(
            os.environ,
            {
                "COINBASE_CAPSULE_ACTIVE_KEY_ID": "v1",
                "COINBASE_CAPSULE_KEYS_JSON": json.dumps(
                    {"v1": base64.urlsafe_b64encode(b"b" * 32).decode().rstrip("=")}
                ),
            },
        )
        self.environment.start()

    def tearDown(self):
        self.environment.stop()

    def test_round_trip_uses_bybit_specific_capsule(self):
        capsule = encrypt_bybit_credentials("api-key", "api-secret")
        credentials = decrypt_bybit_credentials(capsule)

        self.assertTrue(capsule.startswith("dhb1.v1."))
        self.assertEqual(credentials.api_key, "api-key")
        self.assertEqual(credentials.api_secret, "api-secret")

    def test_rejects_tampered_capsule(self):
        capsule = encrypt_bybit_credentials("api-key", "api-secret")
        tampered = f"{capsule[:-1]}{'A' if capsule[-1] != 'A' else 'B'}"

        with self.assertRaises(HTTPException) as context:
            decrypt_bybit_credentials(tampered)

        self.assertEqual(context.exception.status_code, 400)
        self.assertNotIn("api-secret", context.exception.detail)


class BybitSigningTest(unittest.TestCase):
    def test_matches_documented_hmac_shape(self):
        headers = _signed_headers(
            "api-key",
            "api-secret",
            "accountType=UNIFIED",
            timestamp_ms=1_700_000_000_000,
        )

        self.assertEqual(headers["X-BAPI-API-KEY"], "api-key")
        self.assertEqual(headers["X-BAPI-TIMESTAMP"], "1700000000000")
        self.assertEqual(
            headers["X-BAPI-SIGN"],
            "6bdbe0630a84a0daf6a23cf721ad1b25f039e63094d8b4df277d6b5bf1fd14d6",
        )

    def test_supports_only_allowlisted_regional_hosts(self):
        self.assertEqual(_api_base_url("indonesia"), "https://api.bybit.id")
        with self.assertRaises(HTTPException):
            _api_base_url("https://attacker.example")


class BybitCsvTest(unittest.TestCase):
    def test_renders_summary_balances_and_open_positions(self):
        account = {
            "accountType": "UNIFIED",
            "totalEquity": "1000.5",
            "totalWalletBalance": "950",
            "totalAvailableBalance": "700",
            "coin": [
                {
                    "coin": "USDT",
                    "equity": "900",
                    "usdValue": "900",
                    "walletBalance": "900",
                    "marginCollateral": True,
                    "collateralSwitch": True,
                }
            ],
        }
        positions = [
            (
                "linear",
                {
                    "symbol": "BTCUSDT",
                    "side": "Buy",
                    "size": "0.1",
                    "positionIdx": 0,
                    "positionValue": "7000",
                    "avgPrice": "69000",
                    "markPrice": "70000",
                    "unrealisedPnl": "100",
                    "createdTime": "1700000000000",
                },
            ),
            ("linear", {"symbol": "ETHUSDT", "side": "", "size": "0"}),
        ]

        rows = list(csv.DictReader(io.StringIO(_render_bybit_csv(account, positions))))

        self.assertEqual([row["row_type"] for row in rows], [
            "account_summary",
            "balance",
            "position",
        ])
        self.assertEqual(rows[0]["id"], "bybit:unified")
        self.assertEqual(rows[1]["id"], "bybit:balance:USDT")
        self.assertEqual(rows[2]["id"], "bybit:position:linear:BTCUSDT:0")
        self.assertEqual(rows[2]["side"], "long")
        self.assertEqual(rows[2]["unrealised_pnl"], "100")

    def test_renders_total_and_funding_balances(self):
        account = {
            "accountType": "UNIFIED",
            "totalEquity": "100.5",
            "coin": [],
        }
        funding = [
            {"coin": "USDT", "walletBalance": "236", "transferBalance": "236"},
            {"coin": "BTC", "walletBalance": "0.01", "transferBalance": "0"},
        ]

        rows = list(
            csv.DictReader(
                io.StringIO(
                    _render_bybit_csv(
                        account,
                        [],
                        funding,
                        {"BTC": Decimal("70000")},
                    )
                )
            )
        )

        self.assertEqual(rows[0]["id"], "bybit:total")
        self.assertEqual(rows[0]["total_equity_usd"], "1036.50")
        self.assertEqual(rows[1]["id"], "bybit:unified")
        self.assertEqual(rows[2]["id"], "bybit:funding:balance:USDT")
        self.assertEqual(rows[2]["usd_value"], "236")
        self.assertEqual(rows[3]["id"], "bybit:funding:balance:BTC")
        self.assertEqual(rows[3]["usd_value"], "700.00")


class BybitRouteTest(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_funding_and_public_spot_prices(self):
        with patch(
            "routers.bybit._get_bybit_json",
            AsyncMock(
                return_value={
                    "result": {
                        "balance": [{"coin": "USDT", "walletBalance": "2"}]
                    }
                }
            ),
        ) as signed:
            balances = await _fetch_funding_balances(
                object(), "https://api.bybit.com", "key", "secret"
            )

        self.assertEqual(balances[0]["coin"], "USDT")
        signed.assert_awaited_once_with(
            unittest.mock.ANY,
            "https://api.bybit.com",
            "/v5/asset/transfer/query-account-coins-balance",
            "key",
            "secret",
            {"accountType": "FUND"},
        )

        with patch(
            "routers.bybit._get_bybit_public_json",
            AsyncMock(
                return_value={
                    "result": {
                        "list": [
                            {
                                "symbol": "BTCUSDT",
                                "lastPrice": "69900",
                                "usdIndexPrice": "70000",
                            }
                        ]
                    }
                }
            ),
        ):
            prices = await _fetch_spot_usd_prices(
                object(), "https://api.bybit.com"
            )

        self.assertEqual(prices["BTC"], Decimal("70000"))

    async def test_capsule_requires_read_only_validation(self):
        request = BybitCapsuleRequest(
            api_key="api-key",
            api_secret="api-secret",
            region="global",
        )
        with (
            patch(
                "routers.bybit._validate_view_only_credentials",
                AsyncMock(return_value={"read_only": True}),
            ) as validate,
            patch(
                "routers.bybit.encrypt_bybit_credentials",
                return_value="dhb1.v1.encrypted",
            ),
        ):
            response = await create_bybit_capsule(request)

        validate.assert_awaited_once()
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(json.loads(response.body)["capsule"], "dhb1.v1.encrypted")

    async def test_account_route_fetches_balances_and_position_categories(self):
        credentials = type(
            "Credentials",
            (),
            {"api_key": "api-key", "api_secret": "api-secret"},
        )()
        account = {"accountType": "UNIFIED", "totalEquity": "10", "coin": []}
        funding = [{"coin": "USDT", "walletBalance": "2"}]
        with (
            patch("routers.bybit.decrypt_bybit_credentials", return_value=credentials),
            patch(
                "routers.bybit._fetch_wallet_balance",
                AsyncMock(return_value=account),
            ),
            patch(
                "routers.bybit._fetch_position_category",
                AsyncMock(return_value=[]),
            ) as fetch_positions,
            patch(
                "routers.bybit._fetch_funding_balances",
                AsyncMock(return_value=funding),
            ) as fetch_funding,
            patch(
                "routers.bybit._fetch_spot_usd_prices",
                AsyncMock(return_value={}),
            ) as fetch_prices,
        ):
            response = await get_bybit_account_csv(
                "dhb1.v1.encrypted", "global", True
            )

        self.assertEqual(fetch_positions.await_count, 3)
        fetch_funding.assert_awaited_once()
        fetch_prices.assert_awaited_once()
        self.assertEqual(response.media_type, "text/csv")
        self.assertIn("bybit:total", response.body.decode())
        self.assertIn("bybit:unified", response.body.decode())


if __name__ == "__main__":
    unittest.main()
