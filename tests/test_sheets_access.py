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
from models import Account, AccountAddress, AccountToken
from routers.auth import router as auth_router
from security import create_access_token, create_sheets_access_token
from value_rate_limit import ValueRateLimitMiddleware


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

    def test_rate_limits_anonymous_and_authenticated_requests_separately(self):
        app = FastAPI()
        app.add_middleware(
            ValueRateLimitMiddleware,
            authenticated_limit=3,
            anonymous_limit=2,
            window_seconds=60,
            session_factory=self.Session,
        )

        @app.get("/v/{resource_id}")
        async def value(resource_id: str):
            return Response(content=resource_id, media_type="text/csv")

        with patch("value_rate_limit.get_redis_client", return_value=None):
            with TestClient(app) as client:
                anonymous = [client.get("/v/AbCdEf123456") for _ in range(3)]
                self.assertEqual(
                    [response.status_code for response in anonymous],
                    [200, 200, 429],
                )
                self.assertEqual(anonymous[0].headers["x-ratelimit-limit"], "2")
                self.assertEqual(anonymous[2].headers["retry-after"], "60")

                with self.Session() as db:
                    sheet_row = AccountToken(
                        id="sheets-one",
                        account_id="account-one",
                        created_at=2,
                        is_active=True,
                        purpose="sheets",
                    )
                    db.add(sheet_row)
                    db.commit()
                sheet_token = create_sheets_access_token(
                    {"sub": "account-one", "jti": "sheets-one"}
                )
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

                invalid = client.get(
                    "/v/AbCdEf123456",
                    params={"auth_token": "not-a-token"},
                )
                self.assertEqual(invalid.status_code, 401)

    def test_cached_value_reads_still_consume_the_request_limit(self):
        app = FastAPI()
        calls = {"value": 0}
        app.add_middleware(CSVCacheMiddleware, ttl_seconds=60)
        app.add_middleware(
            ValueRateLimitMiddleware,
            authenticated_limit=3,
            anonymous_limit=2,
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
                    first = client.get("/v/AbCdEf123456")
                    second = client.get("/v/AbCdEf123456")
                    limited = client.get("/v/AbCdEf123456")

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(second.headers["x-csv-cache"], "HIT")
        self.assertEqual(limited.status_code, 429)
        self.assertEqual(calls["value"], 1)


if __name__ == "__main__":
    unittest.main()
