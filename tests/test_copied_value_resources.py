import unittest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI, Response
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base, get_db
from dependencies import get_current_account
from csv_cache import CachedCSVPreview, csv_cache_digest
from models import Account, AccountValueResource, ValueResource
from routers.value import router


class CopiedValueResourcesTest(unittest.TestCase):
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
            expire_on_commit=False,
            bind=self.engine,
        )
        db = self.Session()
        try:
            self.first_account = Account(
                id="account-one",
                init_address="0x1111111111111111111111111111111111111111",
                init_address_network="evm",
            )
            self.second_account = Account(
                id="account-two",
                init_address="0x2222222222222222222222222222222222222222",
                init_address_network="evm",
            )
            db.add_all([self.first_account, self.second_account])
            db.commit()
        finally:
            db.close()

        self.current_account = self.first_account
        self.app = FastAPI()
        self.app.include_router(router)

        def override_db():
            db = self.Session()
            try:
                yield db
            finally:
                db.close()

        def override_account():
            return self.current_account

        self.app.dependency_overrides[get_db] = override_db
        self.app.dependency_overrides[get_current_account] = override_account
        self.client = TestClient(self.app)

    def tearDown(self):
        self.client.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _create_resource(
        self,
        *,
        source="morpho",
        key="vault:one",
        column="supply_usd",
        parameters=None,
    ):
        response = self.client.post(
            "/value-resources",
            json={
                "source": source,
                "key": key,
                "column": column,
                "parameters": parameters
                or {
                    "address": "0x1111111111111111111111111111111111111111",
                    "chain_id": "1",
                },
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["id"]

    def _record_copy(self, resource_id):
        response = self.client.post(f"/value-resources/{resource_id}/copies")
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def _delete_copy(self, resource_id):
        return self.client.delete(f"/value-resources/{resource_id}/copies")

    def test_repeated_copies_upsert_one_history_row(self):
        resource_id = self._create_resource()
        with patch("routers.value._request_source", new=AsyncMock()) as source_request:
            first = self._record_copy(resource_id)
            source_request.assert_not_awaited()
        db = self.Session()
        try:
            history = db.query(AccountValueResource).one()
            history.first_copied_at = 100
            history.last_copied_at = 100
            db.commit()
        finally:
            db.close()
        second = self._record_copy(resource_id)

        self.assertEqual(first["copy_count"], 1)
        self.assertEqual(second["copy_count"], 2)
        self.assertEqual(second["first_copied_at"], 100)
        self.assertGreater(second["last_copied_at"], 100)
        db = self.Session()
        try:
            self.assertEqual(db.query(AccountValueResource).count(), 1)
            self.assertEqual(db.query(ValueResource).count(), 1)
        finally:
            db.close()

    def test_history_is_isolated_per_account_while_resource_is_shared(self):
        resource_id = self._create_resource()
        self._record_copy(resource_id)

        self.current_account = self.second_account
        self.assertEqual(self.client.get("/value-resources/mine").json()["total"], 0)
        self._record_copy(resource_id)

        db = self.Session()
        try:
            self.assertEqual(db.query(ValueResource).count(), 1)
            self.assertEqual(db.query(AccountValueResource).count(), 2)
        finally:
            db.close()

    def test_delete_removes_only_current_history_and_keeps_resource_resolvable(self):
        resource_id = self._create_resource()
        self._record_copy(resource_id)
        self.current_account = self.second_account
        self._record_copy(resource_id)

        self.current_account = self.first_account
        with patch("routers.value._request_source", new=AsyncMock()) as source_request:
            deleted = self._delete_copy(resource_id)

        self.assertEqual(deleted.status_code, 204)
        self.assertEqual(deleted.content, b"")
        source_request.assert_not_awaited()
        db = self.Session()
        try:
            self.assertIsNotNone(db.get(ValueResource, resource_id))
            histories = db.query(AccountValueResource).all()
            self.assertEqual(len(histories), 1)
            self.assertEqual(histories[0].account_id, self.second_account.id)
            self.assertEqual(histories[0].resource_id, resource_id)
        finally:
            db.close()
        with patch(
            "routers.value._resolve_stable_value",
            new=AsyncMock(return_value=Response(content="123.45", media_type="text/csv")),
        ) as resolve_value:
            resolved = self.client.get(f"/v/{resource_id}")
        self.assertEqual(resolved.status_code, 200, resolved.text)
        self.assertEqual(resolved.text, "123.45")
        resolve_value.assert_awaited_once()

    def test_delete_rejects_missing_or_another_accounts_history(self):
        resource_id = self._create_resource()
        self.current_account = self.second_account
        self._record_copy(resource_id)

        self.current_account = self.first_account
        other_account = self._delete_copy(resource_id)
        missing = self._delete_copy("AbCdEf123456")

        self.assertEqual(other_account.status_code, 404)
        self.assertEqual(missing.status_code, 404)
        db = self.Session()
        try:
            self.assertEqual(db.query(AccountValueResource).count(), 1)
            self.assertEqual(db.query(AccountValueResource).one().account_id, self.second_account.id)
        finally:
            db.close()

    def test_copy_can_recreate_history_after_delete(self):
        resource_id = self._create_resource()
        self._record_copy(resource_id)

        self.assertEqual(self._delete_copy(resource_id).status_code, 204)
        recreated = self._record_copy(resource_id)

        self.assertEqual(recreated["copy_count"], 1)
        db = self.Session()
        try:
            self.assertEqual(db.query(AccountValueResource).count(), 1)
            self.assertEqual(db.query(AccountValueResource).one().resource_id, resource_id)
        finally:
            db.close()

    def test_list_is_newest_first_and_paginated(self):
        first_id = self._create_resource(key="vault:first")
        second_id = self._create_resource(key="vault:second")
        self._record_copy(first_id)
        self._record_copy(second_id)
        db = self.Session()
        try:
            first = db.query(AccountValueResource).filter_by(resource_id=first_id).one()
            second = db.query(AccountValueResource).filter_by(resource_id=second_id).one()
            first.last_copied_at = 100
            second.last_copied_at = 200
            db.commit()
        finally:
            db.close()

        response = self.client.get("/value-resources/mine?limit=1&offset=0")

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["total"], 2)
        self.assertEqual(payload["limit"], 1)
        self.assertEqual(payload["offset"], 0)
        self.assertEqual([item["id"] for item in payload["items"]], [second_id])

    def test_list_returns_only_safe_resource_metadata(self):
        resource_id = self._create_resource(
            source="coinbase",
            key="coinbase:total_balance",
            column="balance",
            parameters={"include_portfolios": "true"},
        )
        self._record_copy(resource_id)

        payload = self.client.get("/value-resources/mine").json()
        serialized = str(payload)

        self.assertEqual(payload["items"][0]["parameters"], {"include_portfolios": "true"})
        self.assertEqual(
            payload["items"][0]["credential_parameters"],
            ["capsule", "intx_capsule"],
        )
        self.assertNotIn("auth_token", serialized)
        self.assertNotIn("dhc1.", serialized)

    def test_previews_return_cached_value_and_data_freshness_only(self):
        resource_id = self._create_resource()
        self._record_copy(resource_id)
        cached = {
            resource_id: CachedCSVPreview(
                body=b"12966.07",
                updated_at=1700000000,
                cache_status="stale",
            )
        }

        with (
            patch("routers.value.get_redis_client", return_value=object()),
            patch(
                "routers.value.load_cached_csv_previews",
                new=AsyncMock(return_value=cached),
            ) as load_previews,
            patch("routers.value._request_source", new=AsyncMock()) as source_request,
        ):
            response = self.client.post(
                "/value-resources/previews",
                json={"resource_ids": [resource_id], "credentials": {}},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            response.json()["items"],
            [
                {
                    "id": resource_id,
                    "value": "12966.07",
                    "data_updated_at": 1700000000,
                    "cache_status": "stale",
                }
            ],
        )
        source_request.assert_not_awaited()
        cache_requests = load_previews.await_args.args[1]
        self.assertEqual(cache_requests[0][0], resource_id)
        self.assertEqual(cache_requests[0][1], f"/v/{resource_id}")
        self.assertEqual(cache_requests[0][2], [])

    def test_preview_cache_lookup_matches_short_url_without_resource_parameters(self):
        resource_id = self._create_resource(
            source="binance",
            key="binance:total",
            column="total_equity_usd",
            parameters={"include_futures": "true"},
        )
        self._record_copy(resource_id)

        expected_short_url_digest = csv_cache_digest(
            method="GET",
            path=f"/v/{resource_id}",
            query_items=[
                ("capsule", "right-capsule"),
                ("auth_token", "sheets-token"),
            ],
        )

        async def cached_for_capsule(_client, requests, **_kwargs):
            _request_id, path, query_items = requests[0]
            preview_digest = csv_cache_digest(
                method="GET",
                path=path,
                query_items=query_items,
            )
            if preview_digest != expected_short_url_digest:
                return {}
            return {
                resource_id: CachedCSVPreview(
                    body=b"236.70",
                    updated_at=1700000000,
                    cache_status="fresh",
                )
            }

        with (
            patch("routers.value.get_redis_client", return_value=object()),
            patch(
                "routers.value.load_cached_csv_previews",
                new=AsyncMock(side_effect=cached_for_capsule),
            ),
            patch("routers.value._request_source", new=AsyncMock()) as source_request,
        ):
            missing = self.client.post(
                "/value-resources/previews",
                json={
                    "resource_ids": [resource_id],
                    "credentials": {"binance": {"capsule": "wrong-capsule"}},
                },
            )
            found = self.client.post(
                "/value-resources/previews",
                json={
                    "resource_ids": [resource_id],
                    "credentials": {"binance": {"capsule": "right-capsule"}},
                },
            )

        self.assertEqual(missing.json()["items"][0]["cache_status"], "missing")
        self.assertEqual(found.json()["items"][0]["value"], "236.70")
        self.assertNotIn("capsule", found.text)
        source_request.assert_not_awaited()

    def test_preview_rejects_resources_from_another_account(self):
        resource_id = self._create_resource()
        self._record_copy(resource_id)
        self.current_account = self.second_account

        response = self.client.post(
            "/value-resources/previews",
            json={"resource_ids": [resource_id]},
        )

        self.assertEqual(response.status_code, 404)

    def test_history_endpoints_require_authentication(self):
        resource_id = self._create_resource()
        del self.app.dependency_overrides[get_current_account]

        listed = self.client.get("/value-resources/mine")
        recorded = self.client.post(f"/value-resources/{resource_id}/copies")
        deleted = self.client.delete(f"/value-resources/{resource_id}/copies")
        previewed = self.client.post(
            "/value-resources/previews",
            json={"resource_ids": [resource_id]},
        )

        self.assertEqual(listed.status_code, 401)
        self.assertEqual(recorded.status_code, 401)
        self.assertEqual(deleted.status_code, 401)
        self.assertEqual(previewed.status_code, 401)


if __name__ == "__main__":
    unittest.main()
