import unittest
from unittest.mock import patch

import jwt
from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from config import ALGORITHM, SECRET_KEY
from csv_cache import CSVCacheMiddleware
from database import Base, get_db
from models import Account, AccountAddress, AccountToken, UsageDaily, ValueResource
from routers.auth import router as auth_router
from security import create_access_token, create_sheets_access_token
from value_rate_limit import (
    DATA_ACCESS_INTERNAL_HEADER,
    DATA_ACCESS_INTERNAL_TOKEN,
    ValueRateLimitMiddleware,
)


class SheetsAccessTest(unittest.TestCase):
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
        with self.Session() as db:
            db.add(
                Account(
                    id="account-one",
                    init_address="0x1111111111111111111111111111111111111111",
                    init_address_network="eth",
                )
            )
            db.add(
                AccountAddress(
                    account_id="account-one",
                    address="0x1111111111111111111111111111111111111111",
                    network="eth",
                    can_auth=True,
                )
            )
            db.add(
                AccountToken(
                    id="session-one",
                    account_id="account-one",
                    created_at=1,
                    is_active=True,
                    purpose="session",
                )
            )
            db.commit()

        self.session_token = create_access_token(
            {"sub": "account-one", "jti": "session-one"}
        )

    def tearDown(self):
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _override_db(self):
        db = self.Session()
        try:
            yield db
        finally:
            db.close()

    def _create_sheets_token(self, token_id: str = "sheets-one") -> str:
        with self.Session() as db:
            db.add(
                AccountToken(
                    id=token_id,
                    account_id="account-one",
                    created_at=2,
                    is_active=True,
                    purpose="sheets",
                )
            )
            db.commit()
        return create_sheets_access_token(
            {"sub": "account-one", "jti": token_id}
        )

    def test_scoped_token_is_stable_non_expiring_and_cannot_manage_account(self):
        app = FastAPI()
        app.include_router(auth_router)
        app.dependency_overrides[get_db] = self._override_db

        with TestClient(app) as client:
            headers = {"Authorization": f"Bearer {self.session_token}"}
            first = client.post("/web3/sheets-token", headers=headers)
            second = client.post("/web3/sheets-token", headers=headers)

            self.assertEqual(first.status_code, 200, first.text)
            self.assertEqual(first.json(), second.json())
            sheets_token = first.json()["access_token"]
            payload = jwt.decode(
                sheets_token,
                SECRET_KEY,
                algorithms=[ALGORITHM],
            )
            self.assertEqual(payload["scope"], "sheets")
            self.assertNotIn("exp", payload)

            denied = client.get(
                "/web3/addresses",
                headers={"Authorization": f"Bearer {sheets_token}"},
            )
            self.assertEqual(denied.status_code, 401, denied.text)

            sessions = client.get("/web3/tokens", headers=headers)
            purposes = {item["purpose"] for item in sessions.json()}
            self.assertEqual(purposes, {"session", "sheets"})

    def test_rejects_anonymous_requests_and_rate_limits_authenticated_reads(self):
        app = FastAPI()
        app.add_middleware(
            ValueRateLimitMiddleware,
            authenticated_limit=3,
            window_seconds=60,
            session_factory=self.Session,
        )

        @app.get("/v/{resource_id}")
        async def value(resource_id: str):
            return Response(content=resource_id, media_type="text/csv")

        with patch("value_rate_limit.get_redis_client", return_value=None):
            with TestClient(app) as client:
                anonymous = client.get("/v/AbCdEf123456")
                self.assertEqual(anonymous.status_code, 401)
                self.assertIn("Authentication required", anonymous.text)

                sheet_token = self._create_sheets_token()
                authenticated = [
                    client.get(
                        "/v/AbCdEf123456",
                        params={"auth_token": sheet_token},
                    )
                    for _ in range(4)
                ]
                self.assertEqual(
                    [response.status_code for response in authenticated],
                    [200, 200, 200, 429],
                )
                self.assertEqual(authenticated[0].headers["x-ratelimit-limit"], "3")
                self.assertEqual(
                    authenticated[0].headers["cache-control"],
                    "private, max-age=60",
                )

                invalid = client.get(
                    "/v/AbCdEf123456",
                    params={"auth_token": "not-a-token"},
                )
                self.assertEqual(invalid.status_code, 401)

        with self.Session() as db:
            usage = db.query(UsageDaily).all()
            self.assertEqual(
                sum(item.request_count for item in usage),
                4,
            )
            self.assertEqual({item.account_id for item in usage}, {"account-one"})
            self.assertEqual({item.source for item in usage}, {"short-value"})
            self.assertEqual(
                {item.status_group for item in usage},
                {"success", "client_error"},
            )

    def test_tracks_resolved_short_value_source_without_sensitive_data(self):
        app = FastAPI()
        app.add_middleware(
            ValueRateLimitMiddleware,
            authenticated_limit=5,
            window_seconds=60,
            session_factory=self.Session,
        )

        @app.get("/v/{resource_id}")
        async def value(resource_id: str):
            return Response(
                content=resource_id,
                media_type="text/csv",
                headers={"X-Value-Source": "uniswap"},
            )

        with patch("value_rate_limit.get_redis_client", return_value=None):
            with TestClient(app) as client:
                sheet_token = self._create_sheets_token("sheets-analytics")
                response = client.get(
                    "/v/AbCdEf123456",
                    params={
                        "auth_token": sheet_token,
                        "capsule": "must-not-be-stored",
                    },
                )

        self.assertEqual(response.status_code, 200)
        with self.Session() as db:
            usage = db.query(UsageDaily).one()
            self.assertEqual(usage.account_id, "account-one")
            self.assertEqual(usage.source, "uniswap")
            self.assertEqual(usage.status_group, "success")
            self.assertEqual(usage.request_count, 1)

    def test_resolves_failed_short_value_source_from_saved_resource(self):
        resource_id = "MorphoFail12"
        with self.Session() as db:
            db.add(
                ValueResource(
                    id=resource_id,
                    fingerprint="f" * 64,
                    source="morpho",
                    key="position-one",
                    column="supply_usd",
                    parameters={},
                    created_at=1,
                )
            )
            db.commit()

        app = FastAPI()
        app.add_middleware(
            ValueRateLimitMiddleware,
            authenticated_limit=5,
            window_seconds=60,
            session_factory=self.Session,
        )

        @app.get("/v/{current_resource_id}")
        async def value(current_resource_id: str):
            return Response(
                content="upstream unavailable",
                status_code=502,
                media_type="text/plain",
            )

        with patch("value_rate_limit.get_redis_client", return_value=None):
            with TestClient(app) as client:
                sheet_token = self._create_sheets_token("sheets-failed-source")
                response = client.get(
                    f"/v/{resource_id}",
                    params={"auth_token": sheet_token},
                )

        self.assertEqual(response.status_code, 502)
        with self.Session() as db:
            usage = db.query(UsageDaily).one()
            self.assertEqual(usage.source, "morpho")
            self.assertEqual(usage.status_group, "server_error")

    def test_protects_direct_data_routes_and_allows_internal_resolution(self):
        app = FastAPI()
        app.add_middleware(
            ValueRateLimitMiddleware,
            authenticated_limit=5,
            window_seconds=60,
            session_factory=self.Session,
        )

        @app.get("/stablecoins/balances.csv")
        async def balances():
            return Response(content="balance", media_type="text/csv")

        @app.get("/health/readiness")
        async def readiness():
            return {"status": "ready"}

        @app.post("/value-resources")
        async def create_resource():
            return {"id": "resource"}

        sheet_token = self._create_sheets_token()
        with patch("value_rate_limit.get_redis_client", return_value=None):
            with TestClient(app) as client:
                anonymous = client.get("/stablecoins/balances.csv")
                scoped = client.get(
                    "/stablecoins/balances.csv",
                    params={"auth_token": sheet_token},
                )
                session = client.get(
                    "/stablecoins/balances.csv",
                    headers={"Authorization": f"Bearer {self.session_token}"},
                )
                internal = client.get(
                    "/stablecoins/balances.csv",
                    headers={
                        DATA_ACCESS_INTERNAL_HEADER: DATA_ACCESS_INTERNAL_TOKEN
                    },
                )
                public_health = client.get("/health/readiness")
                scoped_write = client.post(
                    "/value-resources",
                    params={"auth_token": sheet_token},
                )
                session_write = client.post(
                    "/value-resources",
                    headers={"Authorization": f"Bearer {self.session_token}"},
                )

        self.assertEqual(anonymous.status_code, 401)
        self.assertEqual(scoped.status_code, 200)
        self.assertEqual(session.status_code, 200)
        self.assertEqual(internal.status_code, 200)
        self.assertEqual(public_health.status_code, 200)
        self.assertEqual(scoped_write.status_code, 401)
        self.assertEqual(session_write.status_code, 200)

    def test_cached_value_reads_still_consume_the_request_limit(self):
        app = FastAPI()
        calls = {"value": 0}
        app.add_middleware(CSVCacheMiddleware, ttl_seconds=60)
        app.add_middleware(
            ValueRateLimitMiddleware,
            authenticated_limit=2,
            window_seconds=60,
            session_factory=self.Session,
        )

        @app.get("/v/{resource_id}")
        async def value(resource_id: str):
            calls["value"] += 1
            return Response(content=resource_id, media_type="text/csv")

        with patch("value_rate_limit.get_redis_client", return_value=None):
            with patch("csv_cache.get_redis_client", return_value=None):
                with TestClient(app) as client:
                    sheet_token = self._create_sheets_token("sheets-cache")
                    params = {"auth_token": sheet_token}
                    first = client.get("/v/AbCdEf123456", params=params)
                    second = client.get("/v/AbCdEf123456", params=params)
                    limited = client.get("/v/AbCdEf123456", params=params)

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(second.headers["x-csv-cache"], "HIT")
        self.assertEqual(limited.status_code, 429)
        self.assertEqual(calls["value"], 1)


if __name__ == "__main__":
    unittest.main()
