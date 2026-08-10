import uuid
from sqlalchemy import Column, String, Boolean, ForeignKey, Integer, Float
from sqlalchemy.orm import relationship
from database import Base

class Account(Base):
    __tablename__ = "account"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    init_address = Column(String, nullable=False)
    init_address_network = Column(String, nullable=False)
    balance = Column(Float, default=0.0)

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

    # But for compatibility with JWT `iat`, Integer (seconds) is fine.

