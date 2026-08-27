import unittest
from unittest.mock import patch

from eth_account import Account as EthAccount
from eth_account.messages import encode_defunct
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base, get_db
from google_identity import (
    GoogleIdentityNotConfiguredError,
    GoogleIdentityUnavailableError,
    GoogleIdentityVerificationError,
)
from models import Account, AccountAddress, AccountIdentity, AccountToken
from routers.auth import router


class AuthTest(unittest.TestCase):
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
        self.app.dependency_overrides[get_db] = self._override_db
        self.client = TestClient(self.app)
        self.wallet = EthAccount.create()

    def tearDown(self):
        self.client.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _override_db(self):
        db = self.Session()
        try:
            yield db
        finally:
            db.close()

    def _login_payload(self):
        message = "Login to Data Hunt Web3 Portal"
        signed = self.wallet.sign_message(encode_defunct(text=message))
        return {
            "address": self.wallet.address,
            "message": message,
            "signature": signed.signature.hex(),
        }

    def test_login_marks_newly_created_account(self):
        response = self.client.post("/web3/login", json=self._login_payload())

        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["is_new_account"])
        self.assertEqual(response.json()["token_type"], "bearer")
        self.assertIsInstance(response.json()["access_token"], str)
        with self.Session() as db:
            self.assertEqual(db.query(Account).count(), 1)
            self.assertEqual(db.query(AccountAddress).count(), 1)
            self.assertEqual(db.query(AccountToken).count(), 1)

    def test_login_marks_existing_account_as_returning(self):
        address = self.wallet.address.lower()
        with self.Session() as db:
            account = Account(
                init_address=address,
                init_address_network="eth",
            )
            db.add(account)
            db.commit()
            db.add(
                AccountAddress(
                    account_id=account.id,
                    address=address,
                    network="eth",
                    can_auth=True,
                )
            )
            db.commit()

        response = self.client.post("/web3/login", json=self._login_payload())

        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["is_new_account"])
        with self.Session() as db:
            self.assertEqual(db.query(Account).count(), 1)
            self.assertEqual(db.query(AccountAddress).count(), 1)
            self.assertEqual(db.query(AccountToken).count(), 1)

    @patch("routers.auth.verify_google_identity_token", return_value="google-subject-1")
    def test_google_login_creates_account_and_identity(self, verifier):
        response = self.client.post(
            "/web3/google/login",
            json={"credential": "google-id-token"},
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["is_new_account"])
        self.assertIsInstance(response.json()["access_token"], str)
        verifier.assert_called_once_with("google-id-token")
        with self.Session() as db:
            account = db.query(Account).one()
            identity = db.query(AccountIdentity).one()
            self.assertEqual(account.init_address, f"google:{account.id}")
            self.assertEqual(account.init_address_network, "google")
            self.assertEqual(identity.account_id, account.id)
            self.assertEqual(identity.provider, "google")
            self.assertEqual(identity.subject, "google-subject-1")
            self.assertEqual(db.query(AccountAddress).count(), 0)
            self.assertEqual(db.query(AccountToken).count(), 1)

    @patch("routers.auth.verify_google_identity_token", return_value="google-subject-1")
    def test_google_login_reuses_existing_identity(self, _verifier):
        first = self.client.post(
            "/web3/google/login",
            json={"credential": "first-token"},
        )
        second = self.client.post(
            "/web3/google/login",
            json={"credential": "second-token"},
        )

        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 200, second.text)
        self.assertFalse(second.json()["is_new_account"])
        with self.Session() as db:
            self.assertEqual(db.query(Account).count(), 1)
            self.assertEqual(db.query(AccountIdentity).count(), 1)
            self.assertEqual(db.query(AccountToken).count(), 2)

    @patch(
        "routers.auth.verify_google_identity_token",
        side_effect=GoogleIdentityVerificationError,
    )
    def test_google_login_rejects_invalid_credential(self, _verifier):
        response = self.client.post(
            "/web3/google/login",
            json={"credential": "invalid-token"},
        )

        self.assertEqual(response.status_code, 401, response.text)
        self.assertEqual(response.json()["detail"], "Invalid Google credential")

    @patch(
        "routers.auth.verify_google_identity_token",
        side_effect=GoogleIdentityNotConfiguredError,
    )
    def test_google_login_reports_missing_configuration(self, _verifier):
        response = self.client.post(
            "/web3/google/login",
            json={"credential": "google-id-token"},
        )

        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(response.json()["detail"], "Google login is not configured")

    @patch(
        "routers.auth.verify_google_identity_token",
        side_effect=GoogleIdentityUnavailableError,
    )
    def test_google_login_reports_provider_outage(self, _verifier):
        response = self.client.post(
            "/web3/google/login",
            json={"credential": "google-id-token"},
        )

        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(
            response.json()["detail"],
            "Google login is temporarily unavailable",
        )


if __name__ == "__main__":
    unittest.main()
