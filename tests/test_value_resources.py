import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base, get_db
from models import ValueResource
from routers.value import (
    DIRECT_VALUE_SOURCES,
    RESOURCE_CREDENTIAL_PARAMS,
    RESOURCE_PARAMETER_NAMES,
    VALUE_SOURCES,
    ValueSource,
    router,
)


class ValueResourcesTest(unittest.TestCase):
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
        self.app = FastAPI()

        @self.app.get("/test.csv")
        async def test_csv(value: str = "20", token: str = ""):
            amount = token or value
            return Response(
                content=f"id,amount\none,10\ntwo,{amount}\n",
                media_type="text/csv",
            )

        @self.app.get("/scalar.csv")
        async def scalar_csv(value: str = "42"):
            return Response(content=value, media_type="text/csv")

        self.app.include_router(router)

        def override_db():
            db = self.Session()
            try:
                yield db
            finally:
                db.close()

        self.app.dependency_overrides[get_db] = override_db
        self.client = TestClient(self.app)
        self.patches = [
            patch.dict(VALUE_SOURCES, {"test": ValueSource("/test.csv", "id")}),
            patch.dict(DIRECT_VALUE_SOURCES, {"scalar-test": "/scalar.csv"}),
            patch.dict(
                RESOURCE_PARAMETER_NAMES,
                {
                    "test": frozenset({"value", "token"}),
                    "scalar-test": frozenset({"value"}),
                },
            ),
            patch.dict(
                RESOURCE_CREDENTIAL_PARAMS,
                {"test": frozenset({"token"})},
            ),
        ]
        for current_patch in self.patches:
            current_patch.start()

    def tearDown(self):
        for current_patch in reversed(self.patches):
            current_patch.stop()
        self.client.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _create_stable_resource(self, parameters=None):
        response = self.client.post(
            "/value-resources",
            json={
                "source": "test",
                "key": "two",
                "column": "amount",
                "parameters": parameters or {"value": "123.45"},
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["id"]

    def test_reuses_one_short_id_for_the_same_normalized_request(self):
        first = self._create_stable_resource({"value": " 123.45 "})
        second = self._create_stable_resource({"value": "123.45"})

        self.assertEqual(first, second)
        self.assertEqual(len(first), 12)
        db = self.Session()
        try:
            self.assertEqual(db.query(ValueResource).count(), 1)
            resource = db.get(ValueResource, first)
            self.assertEqual(resource.parameters, {"value": "123.45"})
        finally:
            db.close()

    def test_short_route_resolves_the_saved_stable_cell(self):
        resource_id = self._create_stable_resource()

        response = self.client.get(f"/v/{resource_id}")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.text, "123.45")
        self.assertTrue(response.headers["content-type"].startswith("text/csv"))

    def test_credentials_are_never_stored_and_are_passed_separately(self):
        rejected = self.client.post(
            "/value-resources",
            json={
                "source": "test",
                "key": "two",
                "column": "amount",
                "parameters": {"value": "10", "token": "secret"},
            },
        )
        self.assertEqual(rejected.status_code, 400)
        self.assertIn("must not be stored", rejected.json()["detail"])

        resource_id = self._create_stable_resource({"value": "10"})
        response = self.client.get(f"/v/{resource_id}?token=readonly-secret")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.text, "readonly-secret")
        db = self.Session()
        try:
            resource = db.get(ValueResource, resource_id)
            self.assertNotIn("token", resource.parameters)
        finally:
            db.close()

    def test_coinbase_capsule_cannot_enter_the_resource_table(self):
        response = self.client.post(
            "/value-resources",
            json={
                "source": "coinbase",
                "key": "coinbase:total_balance",
                "column": "balance",
                "parameters": {
                    "capsule": "dhc1.v2.encrypted",
                    "include_portfolios": "true",
                },
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("must not be stored", response.json()["detail"])
        db = self.Session()
        try:
            self.assertEqual(db.query(ValueResource).count(), 0)
        finally:
            db.close()

    def test_both_coinbase_capsules_are_kept_out_of_the_resource_table(self):
        response = self.client.post(
            "/value-resources",
            json={
                "source": "coinbase",
                "key": "coinbase:total_balance",
                "column": "balance",
                "parameters": {
                    "include_portfolios": "true",
                    "intx_capsule": "dhc1.v2.intx-encrypted",
                },
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("must not be stored", response.json()["detail"])

    def test_bybit_capsule_cannot_enter_the_resource_table(self):
        response = self.client.post(
            "/value-resources",
            json={
                "source": "bybit",
                "key": "bybit:unified",
                "column": "total_equity_usd",
                "parameters": {
                    "capsule": "dhb1.v1.encrypted",
                    "region": "global",
                },
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("must not be stored", response.json()["detail"])

    def test_short_route_rejects_parameters_that_do_not_hold_credentials(self):
        resource_id = self._create_stable_resource()

        response = self.client.get(f"/v/{resource_id}?value=override")

        self.assertEqual(response.status_code, 400)
        self.assertIn("Unsupported credential parameter", response.json()["detail"])

    def test_short_route_ignores_the_rate_limit_identity_parameter(self):
        resource_id = self._create_stable_resource()

        response = self.client.get(f"/v/{resource_id}?auth_token=handled-by-middleware")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.text, "123.45")

    def test_direct_single_cell_source_uses_the_same_short_route(self):
        created = self.client.post(
            "/value-resources",
            json={
                "source": "scalar-test",
                "parameters": {"value": "99.5"},
            },
        )
        self.assertEqual(created.status_code, 200, created.text)

        response = self.client.get(f"/v/{created.json()['id']}")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.text, "99.5")


if __name__ == "__main__":
    unittest.main()
