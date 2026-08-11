import base64
import csv
import io
import json
import os
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from binance_capsule import decrypt_binance_credentials, encrypt_binance_credentials
from routers.binance import (
    BinanceCapsuleRequest,
    _asset_usd_value,
    _render_binance_csv,
    _signed_query,
    _validate_read_only_credentials,
    create_binance_capsule,
    get_binance_account_csv,
)


class BinanceCapsuleTest(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(
            os.environ,
            {
                "COINBASE_CAPSULE_ACTIVE_KEY_ID": "v1",
                "COINBASE_CAPSULE_KEYS_JSON": json.dumps(
                    {"v1": base64.urlsafe_b64encode(b"n" * 32).decode().rstrip("=")}
                ),
            },
        )
        self.environment.start()

    def tearDown(self):
        self.environment.stop()

    def test_round_trip_uses_binance_specific_capsule(self):
        capsule = encrypt_binance_credentials("api-key", "api-secret")
        credentials = decrypt_binance_credentials(capsule)

        self.assertTrue(capsule.startswith("dhbn1.v1."))
        self.assertEqual(credentials.api_key, "api-key")
        self.assertEqual(credentials.api_secret, "api-secret")

    def test_rejects_tampered_capsule(self):
        capsule = encrypt_binance_credentials("api-key", "api-secret")
        tampered = f"{capsule[:-1]}{'A' if capsule[-1] != 'A' else 'B'}"

        with self.assertRaises(HTTPException) as context:
            decrypt_binance_credentials(tampered)

        self.assertEqual(context.exception.status_code, 400)
        self.assertNotIn("api-secret", context.exception.detail)


class BinanceSigningTest(unittest.TestCase):
    def test_matches_documented_hmac_shape(self):
        query = _signed_query("api-secret", timestamp_ms=1_700_000_000_000)

        self.assertEqual(
            query,
            "recvWindow=5000&timestamp=1700000000000&signature="
            "46abb9aed31011969cf6ae20983e33c33ecfb85beab54a84d6a06cfb44db3a81",
        )


class BinancePermissionTest(unittest.IsolatedAsyncioTestCase):
    async def test_accepts_read_only_key(self):
        with patch(
            "routers.binance._get_signed_json",
            AsyncMock(
                return_value={
                    "enableReading": True,
                    "enableWithdrawals": False,
                    "enableSpotAndMarginTrading": False,
                    "enableFutures": False,
                }
            ),
        ):
            permissions = await _validate_read_only_credentials(
                object(), "api-key", "api-secret"
            )

        self.assertEqual(permissions, {"read_only": True})

    async def test_rejects_key_with_trading_permission(self):
        with (
            patch(
                "routers.binance._get_signed_json",
                AsyncMock(
                    return_value={
                        "enableReading": True,
                        "enableSpotAndMarginTrading": True,
                    }
                ),
            ),
            self.assertRaises(HTTPException) as context,
        ):
            await _validate_read_only_credentials(object(), "api-key", "api-secret")

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("read-only", context.exception.detail)


class BinanceCsvTest(unittest.TestCase):
    def test_uses_usdc_or_btc_cross_when_usdt_pair_is_missing(self):
        self.assertEqual(
            _asset_usd_value(
                "TOKEN",
                Decimal("2"),
                {
                    "TOKENUSDC": Decimal("3"),
                    "USDCUSDT": Decimal("0.999"),
                },
            ),
            Decimal("5.994"),
        )

    def test_renders_spot_and_futures_rows_with_stable_ids(self):
        spot = {
            "accountType": "SPOT",
            "updateTime": 1_700_000_000_000,
            "balances": [
                {"asset": "BTC", "free": "0.1", "locked": "0.02"},
                {"asset": "USDT", "free": "100", "locked": "0"},
                {"asset": "ETH", "free": "0", "locked": "0"},
            ],
        }
        futures = {
            "totalWalletBalance": "500",
            "totalMarginBalance": "510",
            "availableBalance": "400",
            "totalUnrealizedProfit": "10",
            "assets": [
                {
                    "asset": "USDT",
                    "walletBalance": "500",
                    "marginBalance": "510",
                    "availableBalance": "400",
                    "unrealizedProfit": "10",
                }
            ],
            "positions": [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "0.01",
                    "positionSide": "BOTH",
                    "notional": "700",
                    "entryPrice": "69000",
                    "markPrice": "70000",
                    "unrealizedProfit": "10",
                },
                {"symbol": "ETHUSDT", "positionAmt": "0"},
            ],
        }

        rows = list(
            csv.DictReader(
                io.StringIO(
                    _render_binance_csv(
                        spot, {"BTCUSDT": Decimal("70000")}, futures
                    )
                )
            )
        )

        self.assertEqual(
            [row["row_type"] for row in rows],
            [
                "account_summary",
                "spot_balance",
                "spot_balance",
                "account_summary",
                "futures_balance",
                "futures_position",
            ],
        )
        self.assertEqual(rows[0]["id"], "binance:spot")
        self.assertEqual(rows[0]["total_equity_usd"], "8500")
        self.assertEqual(rows[1]["id"], "binance:spot:balance:BTC")
        self.assertEqual(rows[1]["usd_value"], "8400")
        self.assertEqual(rows[-1]["id"], "binance:futures:position:BTCUSDT:BOTH")
        self.assertEqual(rows[-1]["side"], "long")


class BinanceRouteTest(unittest.IsolatedAsyncioTestCase):
    async def test_capsule_requires_read_only_validation(self):
        request = BinanceCapsuleRequest(api_key="api-key", api_secret="api-secret")
        with (
            patch(
                "routers.binance._validate_read_only_credentials",
                AsyncMock(return_value={"read_only": True}),
            ) as validate,
            patch(
                "routers.binance.encrypt_binance_credentials",
                return_value="dhbn1.v1.encrypted",
            ),
        ):
            response = await create_binance_capsule(request)

        validate.assert_awaited_once()
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(json.loads(response.body)["capsule"], "dhbn1.v1.encrypted")

    async def test_account_route_fetches_spot_prices_and_futures(self):
        credentials = type(
            "Credentials",
            (),
            {"api_key": "api-key", "api_secret": "api-secret"},
        )()
        spot = {"balances": [], "updateTime": 0}
        futures = {"assets": [], "positions": []}
        with (
            patch(
                "routers.binance.decrypt_binance_credentials",
                return_value=credentials,
            ),
            patch(
                "routers.binance._fetch_spot_account", AsyncMock(return_value=spot)
            ) as fetch_spot,
            patch(
                "routers.binance._fetch_spot_prices", AsyncMock(return_value={})
            ) as fetch_prices,
            patch(
                "routers.binance._fetch_futures_account",
                AsyncMock(return_value=futures),
            ) as fetch_futures,
        ):
            response = await get_binance_account_csv("dhbn1.v1.encrypted", True)

        fetch_spot.assert_awaited_once()
        fetch_prices.assert_awaited_once()
        fetch_futures.assert_awaited_once()
        self.assertEqual(response.media_type, "text/csv")
        self.assertIn("binance:spot", response.body.decode())


if __name__ == "__main__":
    unittest.main()
