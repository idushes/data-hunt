import time
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base, get_db
from models import Account, AccountAddress, AccountToken, UsageDaily
from routers import admin_analytics
from security import create_access_token


ADMIN_ADDRESS = "0x1111111111111111111111111111111111111111"
USER_ADDRESS = "0x2222222222222222222222222222222222222222"


class AdminAnalyticsTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
        )
        current_day = int(time.time()) // 86400
        with self.Session() as db:
            for account_id, address, token_id in (
                ("admin-account", ADMIN_ADDRESS, "admin-token"),
                ("user-account", USER_ADDRESS, "user-token"),
            ):
                db.add(
                    Account(
                        id=account_id,
                        init_address=address,
                        init_address_network="eth",
                    )
                )
                db.add(
                    AccountAddress(
                        account_id=account_id,
                        address=address,
                        network="eth",
                        can_auth=True,
                    )
                )
                db.add(
                    AccountToken(
                        id=token_id,
                        account_id=account_id,
                        created_at=1,
                        is_active=True,
                        purpose="session",
                    )
                )
            db.add_all(
                [
                    UsageDaily(
                        day=current_day,
                        account_id="admin-account",
                        source="uniswap",
                        status_group="success",
                        request_count=8,
                    ),
                    UsageDaily(
                        day=current_day,
                        account_id="user-account",
                        source="uniswap",
                        status_group="server_error",
                        request_count=2,
                    ),
                    UsageDaily(
                        day=current_day - 1,
                        account_id="user-account",
                        source="aave",
                        status_group="success",
                        request_count=5,
                    ),
                    UsageDaily(
                        day=current_day - 8,
                        account_id="user-account",
                        source="old-source",
                        status_group="success",
                        request_count=100,
                    ),
                ]
            )
            db.commit()

        self.previous_admins = admin_analytics.FEATURE_REQUEST_ADMIN_ADDRESSES
        admin_analytics.FEATURE_REQUEST_ADMIN_ADDRESSES = frozenset({ADMIN_ADDRESS})
        self.app = FastAPI()
        self.app.include_router(admin_analytics.router)
        self.app.dependency_overrides[get_db] = self._override_db

    def tearDown(self):
        admin_analytics.FEATURE_REQUEST_ADMIN_ADDRESSES = self.previous_admins
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _override_db(self):
        db = self.Session()
        try:
            yield db
        finally:
            db.close()

    @staticmethod
    def _token(account_id: str, token_id: str) -> str:
        return create_access_token({"sub": account_id, "jti": token_id})

    def test_returns_simple_private_usage_summary(self):
        token = self._token("admin-account", "admin-token")
        with TestClient(self.app) as client:
            response = client.get(
                "/admin/analytics",
                params={"days": 7},
                headers={"Authorization": f"Bearer {token}"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers["cache-control"], "private, no-store")
        payload = response.json()
        self.assertEqual(payload["registered_users"], 2)
        self.assertEqual(payload["active_users"], 2)
        self.assertEqual(payload["requests"], 15)
        self.assertEqual(payload["errors"], 2)
        self.assertEqual(payload["success_rate"], 86.7)
        self.assertEqual(len(payload["daily"]), 7)
        self.assertEqual(payload["sources"][0]["source"], "uniswap")
        self.assertEqual(payload["sources"][0]["requests"], 10)
        self.assertNotIn("account_id", response.text)
        self.assertNotIn(ADMIN_ADDRESS, response.text)

    def test_rejects_non_admins_and_unsupported_periods(self):
        user_token = self._token("user-account", "user-token")
        admin_token = self._token("admin-account", "admin-token")
        with TestClient(self.app) as client:
            forbidden = client.get(
                "/admin/analytics",
                headers={"Authorization": f"Bearer {user_token}"},
            )
            invalid_period = client.get(
                "/admin/analytics",
                params={"days": 2},
                headers={"Authorization": f"Bearer {admin_token}"},
            )

        self.assertEqual(forbidden.status_code, 403)
        self.assertEqual(invalid_period.status_code, 400)

    def test_returns_lightweight_admin_access_status(self):
        admin_token = self._token("admin-account", "admin-token")
        user_token = self._token("user-account", "user-token")
        with TestClient(self.app) as client:
            allowed = client.get(
                "/admin/analytics/access",
                headers={"Authorization": f"Bearer {admin_token}"},
            )
            forbidden = client.get(
                "/admin/analytics/access",
                headers={"Authorization": f"Bearer {user_token}"},
            )

        self.assertEqual(allowed.status_code, 200)
        self.assertEqual(allowed.json(), {"is_admin": True})
        self.assertEqual(allowed.headers["cache-control"], "private, no-store")
        self.assertEqual(forbidden.status_code, 403)

    def test_returns_private_live_queue_status_to_admin_only(self):
        admin_token = self._token("admin-account", "admin-token")
        user_token = self._token("user-account", "user-token")
        queue_status = {
            "enabled": True,
            "redis": True,
            "max_wait_seconds": 30,
            "providers": [
                {
                    "provider": "coinbase",
                    "waiting": 3,
                    "in_flight": 2,
                    "concurrency": 4,
                }
            ],
        }
        with (
            patch.object(
                admin_analytics.outbound_queue,
                "status",
                AsyncMock(return_value=queue_status),
            ) as queue_status_mock,
            TestClient(self.app) as client,
        ):
            response = client.get(
                "/admin/analytics/queues",
                headers={"Authorization": f"Bearer {admin_token}"},
            )
            forbidden = client.get(
                "/admin/analytics/queues",
                headers={"Authorization": f"Bearer {user_token}"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers["cache-control"], "private, no-store")
        self.assertEqual(response.json(), queue_status)
        self.assertEqual(forbidden.status_code, 403)
        queue_status_mock.assert_awaited_once_with(include_activity=True)


if __name__ == "__main__":
    unittest.main()
