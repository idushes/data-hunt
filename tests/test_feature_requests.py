import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import routers.feature_requests as feature_requests_router
from database import Base, get_db
from models import Account, AccountAddress, AccountToken
from routers.feature_requests import router
from security import create_access_token


class FeatureRequestsTest(unittest.TestCase):
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
        self.app.include_router(router)

        def override_db():
            db = self.Session()
            try:
                yield db
            finally:
                db.close()

        self.app.dependency_overrides[get_db] = override_db
        self.client = TestClient(self.app)
        self.user_headers = self._create_account(
            "user-account",
            "user-token",
            "0x1111111111111111111111111111111111111111",
        )
        self.other_headers = self._create_account(
            "other-account",
            "other-token",
            "0x2222222222222222222222222222222222222222",
        )
        self.admin_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        self.admin_headers = self._create_account(
            "admin-account",
            "admin-token",
            self.admin_address,
        )
        self.previous_admins = feature_requests_router.FEATURE_REQUEST_ADMIN_ADDRESSES
        feature_requests_router.FEATURE_REQUEST_ADMIN_ADDRESSES = frozenset(
            {self.admin_address}
        )

    def tearDown(self):
        feature_requests_router.FEATURE_REQUEST_ADMIN_ADDRESSES = self.previous_admins
        self.client.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _create_account(self, account_id: str, token_id: str, address: str):
        db = self.Session()
        account = Account(
            id=account_id,
            init_address=address,
            init_address_network="eth",
        )
        db.add(account)
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
            )
        )
        db.commit()
        db.close()
        token = create_access_token(
            {
                "sub": account_id,
                "jti": token_id,
                "address": address,
                "network": "eth",
            }
        )
        return {"Authorization": f"Bearer {token}"}

    def _create_request(self):
        response = self.client.post(
            "/feature-requests",
            headers=self.user_headers,
            json={
                "title": "Add Morpho positions",
                "description": "Supply and borrow positions on Ethereum and Base.",
                "category": "defi_protocol",
            },
        )
        self.assertEqual(response.status_code, 201, response.text)
        return response.json()["item"]

    def test_creation_requires_login_and_prevents_exact_duplicates(self):
        anonymous = self.client.post(
            "/feature-requests",
            json={"title": "Add Morpho", "category": "defi_protocol"},
        )
        self.assertEqual(anonymous.status_code, 401)

        item = self._create_request()
        self.assertEqual(item["status"], "requested")

        duplicate = self.client.post(
            "/feature-requests",
            headers=self.other_headers,
            json={"title": "  ADD   MORPHO positions ", "category": "defi_protocol"},
        )
        self.assertEqual(duplicate.status_code, 409)
        self.assertEqual(duplicate.json()["detail"]["request_id"], item["id"])

    def test_similar_search_finds_existing_request(self):
        item = self._create_request()
        response = self.client.get(
            "/feature-requests/search",
            params={"query": "Morpho protocol"},
        )
        self.assertEqual(response.status_code, 200)
        matches = response.json()["items"]
        self.assertEqual(matches[0]["id"], item["id"])
        self.assertGreater(matches[0]["match_score"], 0.35)

    def test_support_vote_is_unique_and_can_be_removed(self):
        item = self._create_request()
        path = f"/feature-requests/{item['id']}/vote"

        first = self.client.put(path, headers=self.user_headers, json={"active": True})
        repeated = self.client.put(path, headers=self.user_headers, json={"active": True})
        second = self.client.put(path, headers=self.other_headers, json={"active": True})
        removed = self.client.put(path, headers=self.user_headers, json={"active": False})

        self.assertEqual(first.json()["item"]["support_count"], 1)
        self.assertEqual(repeated.json()["item"]["support_count"], 1)
        self.assertEqual(second.json()["item"]["support_count"], 2)
        self.assertEqual(removed.json()["item"]["support_count"], 1)

    def test_only_admin_can_release_and_released_feature_accepts_feedback(self):
        item = self._create_request()
        status_path = f"/feature-requests/{item['id']}/status"
        feedback_path = f"/feature-requests/{item['id']}/feedback"

        early_feedback = self.client.put(
            feedback_path,
            headers=self.user_headers,
            json={"verdict": "works", "comment": "Looks good"},
        )
        self.assertEqual(early_feedback.status_code, 409)

        forbidden = self.client.patch(
            status_path,
            headers=self.user_headers,
            json={"status": "released"},
        )
        self.assertEqual(forbidden.status_code, 403)

        released = self.client.patch(
            status_path,
            headers=self.admin_headers,
            json={"status": "released"},
        )
        self.assertEqual(released.status_code, 200)
        self.assertEqual(released.json()["item"]["status"], "released")

        works = self.client.put(
            feedback_path,
            headers=self.user_headers,
            json={"verdict": "works", "comment": "Balances match the app."},
        )
        self.assertEqual(works.status_code, 200)
        self.assertEqual(works.json()["item"]["works_count"], 1)

        changed = self.client.put(
            feedback_path,
            headers=self.user_headers,
            json={"verdict": "not_working", "comment": "Base position is missing."},
        )
        self.assertEqual(changed.json()["item"]["works_count"], 0)
        self.assertEqual(changed.json()["item"]["not_working_count"], 1)

        comments = self.client.get(feedback_path)
        self.assertEqual(comments.status_code, 200)
        self.assertEqual(comments.json()["items"][0]["verdict"], "not_working")
        self.assertEqual(
            comments.json()["items"][0]["comment"],
            "Base position is missing.",
        )


if __name__ == "__main__":
    unittest.main()
