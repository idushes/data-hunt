import unittest

from eth_account import Account as EthAccount
from eth_account.messages import encode_defunct
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base, get_db
from models import Account, AccountAddress, AccountToken
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


if __name__ == "__main__":
    unittest.main()
