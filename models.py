import uuid
from sqlalchemy import Column, String, Boolean, ForeignKey, Integer
from sqlalchemy.orm import relationship
from database import Base

class Account(Base):
    __tablename__ = "account"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    init_address = Column(String, nullable=False)
    init_address_network = Column(String, nullable=False)

    addresses = relationship("AccountAddress", back_populates="account")

class AccountAddress(Base):
    __tablename__ = "account_address"

    id = Column(Integer, primary_key=True)
    account_id = Column(String, ForeignKey("account.id"))
    address = Column(String, nullable=False)
    network = Column(String, nullable=False)
    can_auth = Column(Boolean, default=False)


    account = relationship("Account", back_populates="addresses")

class AccountToken(Base):
    __tablename__ = "account_token"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    account_id = Column(String, ForeignKey("account.id"), nullable=False, index=True)
    created_at = Column(Integer, nullable=False) # store as timestamp
    is_active = Column(Boolean, default=True)

    # We might want a relationship back to account, but not strictly necessary for the prompt's specific requirements unless we access tokens from account. 
    # Let's add it for completeness if we want to cascade deletes, but for now simple FK is enough. 
    # actually, `created_at` as Integer (unix timestamp) is often easier, or DateTime. 
    # server.py implies we use standard python mostly. Let's use BigInteger or similar if we want. 
    # But for compatibility with JWT `iat`, Integer (seconds) is fine.

