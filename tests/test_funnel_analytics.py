import time
import unittest
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base, get_db
from analytics_retention import AuthFunnelRetention
from models import Account, AccountAddress, AccountToken, AuthFunnelEvent
from routers import admin_analytics, funnel_analytics
from security import create_access_token


ADMIN_ADDRESS = "0x1111111111111111111111111111111111111111"
USER_ADDRESS = "0x2222222222222222222222222222222222222222"


class FunnelAnalyticsTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.app = FastAPI()
        self.app.include_router(funnel_analytics.router)
        self.app.include_router(admin_analytics.router)
        self.app.dependency_overrides[get_db] = self._override_db

        self.previous_admins = admin_analytics.FEATURE_REQUEST_ADMIN_ADDRESSES
        admin_analytics.FEATURE_REQUEST_ADMIN_ADDRESSES = frozenset({ADMIN_ADDRESS})
        self.previous_limiter = funnel_analytics._rate_limiter
        funnel_analytics._rate_limiter = funnel_analytics._AnonymousEventRateLimit()
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
            db.commit()

    def tearDown(self):
        funnel_analytics._rate_limiter = self.previous_limiter
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

    def test_accepts_allowlisted_event_and_deduplicates_daily_retry(self):
        session_id = str(uuid4())
        payload = {
            "session_id": session_id,
            "event": "login_clicked",
            "utm_source": "google",
            "utm_medium": "cpc",
            "utm_campaign": "sheets-search-1",
        }
        with TestClient(self.app) as client:
            first = client.post("/analytics/funnel/events", json=payload)
            retry = client.post("/analytics/funnel/events", json=payload)

        self.assertEqual(first.status_code, 202, first.text)
        self.assertEqual(first.headers["cache-control"], "no-store")
        self.assertEqual(first.json(), {"accepted": True, "deduplicated": False})
        self.assertEqual(retry.status_code, 202, retry.text)
        self.assertEqual(retry.json(), {"accepted": True, "deduplicated": True})
        with self.Session() as db:
            events = db.query(AuthFunnelEvent).all()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].utm_source, "google")
        self.assertEqual(events[0].utm_medium, "cpc")
        self.assertEqual(events[0].utm_campaign, "sheets-search-1")

    def test_product_events_alias_uses_the_same_contract_and_ingestion(self):
        payload = {"session_id": str(uuid4()), "event": "sheets_view"}
        with TestClient(self.app) as client:
            alias = client.post("/product-events", json=payload)
            legacy_retry = client.post("/analytics/funnel/events", json=payload)
            invalid = client.post(
                "/product-events",
                json={"session_id": str(uuid4()), "event": "not-allowlisted"},
            )

        self.assertEqual(alias.status_code, 202, alias.text)
        self.assertEqual(alias.headers["cache-control"], "no-store")
        self.assertEqual(alias.json(), {"accepted": True, "deduplicated": False})
        self.assertEqual(legacy_retry.status_code, 202, legacy_retry.text)
        self.assertEqual(legacy_retry.json(), {"accepted": True, "deduplicated": True})
        self.assertEqual(invalid.status_code, 422, invalid.text)
        with self.Session() as db:
            self.assertEqual(db.query(AuthFunnelEvent).count(), 1)

    def test_product_events_alias_shares_the_anonymous_rate_limit(self):
        funnel_analytics._rate_limiter.session_limit = 1
        session_id = str(uuid4())
        with TestClient(self.app) as client:
            accepted = client.post(
                "/product-events",
                json={"session_id": session_id, "event": "sheets_view"},
            )
            limited = client.post(
                "/analytics/funnel/events",
                json={"session_id": session_id, "event": "login_clicked"},
            )

        self.assertEqual(accepted.status_code, 202, accepted.text)
        self.assertEqual(limited.status_code, 429, limited.text)
        with self.Session() as db:
            self.assertEqual(db.query(AuthFunnelEvent).count(), 1)

    def test_rejects_unknown_sensitive_and_oversized_input(self):
        session_id = str(uuid4())
        with TestClient(self.app) as client:
            unknown = client.post(
                "/analytics/funnel/events",
                json={"session_id": session_id, "event": "anything_else"},
            )
            sensitive = client.post(
                "/analytics/funnel/events",
                json={
                    "session_id": session_id,
                    "event": "login_clicked",
                    "wallet_address": "0x1111111111111111111111111111111111111111",
                },
            )
            oversized = client.post(
                "/analytics/funnel/events",
                json={
                    "session_id": session_id,
                    "event": "login_clicked",
                    "utm_campaign": "a" * 97,
                },
            )
            non_random_session = client.post(
                "/analytics/funnel/events",
                json={
                    "session_id": "00000000-0000-0000-0000-000000000000",
                    "event": "login_clicked",
                },
            )
            arbitrary_source = client.post(
                "/analytics/funnel/events",
                json={
                    "session_id": session_id,
                    "event": "login_clicked",
                    "utm_source": "newsletter",
                    "utm_medium": "referral",
                },
            )
            incomplete_attribution = client.post(
                "/analytics/funnel/events",
                json={
                    "session_id": session_id,
                    "event": "login_clicked",
                    "utm_source": "google",
                },
            )
            non_google_campaign = client.post(
                "/analytics/funnel/events",
                json={
                    "session_id": session_id,
                    "event": "login_clicked",
                    "utm_source": "threads",
                    "utm_medium": "social",
                    "utm_campaign": "campaign-1",
                },
            )

        for response in (
            unknown,
            sensitive,
            oversized,
            non_random_session,
            arbitrary_source,
            incomplete_attribution,
            non_google_campaign,
        ):
            self.assertEqual(response.status_code, 422, response.text)
        with self.Session() as db:
            self.assertEqual(db.query(AuthFunnelEvent).count(), 0)

    def test_rate_limits_each_session_without_network_data(self):
        funnel_analytics._rate_limiter.session_limit = 1
        session_id = str(uuid4())
        with TestClient(self.app) as client:
            accepted = client.post(
                "/analytics/funnel/events",
                json={"session_id": session_id, "event": "sheets_view"},
            )
            limited = client.post(
                "/analytics/funnel/events",
                json={"session_id": session_id, "event": "login_clicked"},
            )

        self.assertEqual(accepted.status_code, 202, accepted.text)
        self.assertEqual(limited.status_code, 429, limited.text)
        self.assertEqual(limited.headers["cache-control"], "no-store")
        self.assertEqual(limited.headers["x-ratelimit-limit"], "240")
        with self.Session() as db:
            self.assertEqual(db.query(AuthFunnelEvent).count(), 1)

    def test_global_quota_bounds_rotating_anonymous_sessions(self):
        funnel_analytics._rate_limiter.global_limit = 1
        with TestClient(self.app) as client:
            accepted = client.post(
                "/analytics/funnel/events",
                json={"session_id": str(uuid4()), "event": "sheets_view"},
            )
            limited = client.post(
                "/analytics/funnel/events",
                json={"session_id": str(uuid4()), "event": "sheets_view"},
            )

        self.assertEqual(accepted.status_code, 202, accepted.text)
        self.assertEqual(limited.status_code, 429, limited.text)
        with self.Session() as db:
            self.assertEqual(db.query(AuthFunnelEvent).count(), 1)

    def test_database_constraints_reject_invalid_event_and_session_shape(self):
        current_day = int(time.time()) // 86400
        with self.Session() as db:
            db.add(
                AuthFunnelEvent(
                    anonymous_session_id=str(uuid4()),
                    day=current_day,
                    event_name="not-allowed",
                )
            )
            with self.assertRaises(IntegrityError):
                db.commit()
            db.rollback()
            db.add(
                AuthFunnelEvent(
                    anonymous_session_id="not-a-session-id",
                    day=current_day,
                    event_name="sheets_view",
                )
            )
            with self.assertRaises(IntegrityError):
                db.commit()

    def test_retention_keeps_only_the_longest_admin_period(self):
        current_day = int(time.time()) // 86400
        with self.Session() as db:
            db.add_all(
                [
                    AuthFunnelEvent(
                        anonymous_session_id=str(uuid4()),
                        day=current_day - 30,
                        event_name="sheets_view",
                    ),
                    AuthFunnelEvent(
                        anonymous_session_id=str(uuid4()),
                        day=current_day - 29,
                        event_name="sheets_view",
                    ),
                ]
            )
            db.commit()

        retention = AuthFunnelRetention(retention_days=30, session_factory=self.Session)
        self.assertEqual(retention.delete_expired(current_day), 1)
        with self.Session() as db:
            self.assertEqual(db.query(AuthFunnelEvent).count(), 1)
            self.assertEqual(db.query(AuthFunnelEvent).one().day, current_day - 29)

    def test_admin_aggregates_steps_and_remains_admin_only(self):
        current_day = int(time.time()) // 86400
        first_session = str(uuid4())
        second_session = str(uuid4())
        with self.Session() as db:
            db.add_all(
                [
                    AuthFunnelEvent(
                        anonymous_session_id=first_session,
                        day=current_day,
                        event_name="sheets_view",
                        utm_source="google",
                    ),
                    AuthFunnelEvent(
                        anonymous_session_id=first_session,
                        day=current_day,
                        event_name="login_clicked",
                        utm_source="google",
                    ),
                    AuthFunnelEvent(
                        anonymous_session_id=second_session,
                        day=current_day,
                        event_name="sheets_view",
                        utm_source="google",
                    ),
                ]
            )
            db.commit()

        admin_token = self._token("admin-account", "admin-token")
        user_token = self._token("user-account", "user-token")
        with TestClient(self.app) as client:
            allowed = client.get(
                "/admin/analytics",
                headers={"Authorization": f"Bearer {admin_token}"},
            )
            forbidden = client.get(
                "/admin/analytics",
                headers={"Authorization": f"Bearer {user_token}"},
            )

        self.assertEqual(allowed.status_code, 200, allowed.text)
        self.assertEqual(forbidden.status_code, 403)
        funnel = allowed.json()["auth_funnel"]
        self.assertEqual(funnel["unique_sessions"], 2)
        steps = {step["event"]: step for step in funnel["steps"]}
        self.assertEqual(steps["sheets_view"], {"event": "sheets_view", "events": 2, "sessions": 2})
        self.assertEqual(steps["login_clicked"], {"event": "login_clicked", "events": 1, "sessions": 1})
        self.assertEqual(steps["wallet_missing"]["events"], 0)
        self.assertNotIn("anonymous_session_id", allowed.text)
        self.assertNotIn(first_session, allowed.text)


if __name__ == "__main__":
    unittest.main()
