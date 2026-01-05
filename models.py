import uuid
from sqlalchemy import Column, String, Boolean, ForeignKey, Integer, Float
from sqlalchemy.dialects.postgresql import JSONB
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


class DebankRequest(Base):
    __tablename__ = "debank_request"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    account_id = Column(String, index=True, nullable=True) # Optional: if linked to a specific user
    path = Column(String, nullable=False)
    params = Column(String, nullable=True) # JSON string of params (e.g. addressed)
    response_json = Column(String, nullable=True) # TEXT or Large String
    status = Column(String, default="pending") # success, error, pending
    cost = Column(Integer, nullable=True) # Cost of query, if we want to track
    created_at = Column(Integer, nullable=False)


class ProjectDict(Base):
    __tablename__ = "project_dict"

    id = Column(String, primary_key=True, unique=True) # e.g. "cowswap"
    chain = Column(String, nullable=True) # e.g. "eth"
    logo_url = Column(String, nullable=True)
    name = Column(String, nullable=True)
    site_url = Column(String, nullable=True)


class TokenDict(Base):
    __tablename__ = "token_dict"

    id = Column(String, primary_key=True) # The address of the token contract
    chain = Column(String, nullable=False) # The chain's name
    name = Column(String, nullable=True)
    symbol = Column(String, nullable=True)
    display_symbol = Column(String, nullable=True)
    optimized_symbol = Column(String, nullable=True) # optimized_symbol || display_symbol || symbol
    decimals = Column(Integer, nullable=True)
    logo_url = Column(String, nullable=True)
    protocol_id = Column(String, nullable=True)
    price = Column(Float, default=0.0) # USD price. Price of 0 means no data.
    price_24h_change = Column(Float, nullable=True)
    is_verified = Column(Boolean, nullable=True)
    is_core = Column(Boolean, nullable=True)
    is_wallet = Column(Boolean, nullable=True)
    is_scam = Column(Boolean, nullable=True)
    is_suspicious = Column(Boolean, nullable=True)
    credit_score = Column(Float, nullable=True)
    total_supply = Column(Float, nullable=True)
    time_at = Column(Float, nullable=True) # Timestamp. Float to handle "1543095952.0"


class CEXDict(Base):
    __tablename__ = "cex_dict"

    id = Column(String, primary_key=True) # address string, e.g. "0x..."
    cex_id = Column(String, nullable=False) # e.g. "coinbase"
    name = Column(String, nullable=True)
    logo_url = Column(String, nullable=True)
    is_deposit = Column(Boolean, nullable=True)
    is_collect = Column(Boolean, nullable=True)
    is_gastopup = Column(Boolean, nullable=True)
    is_vault = Column(Boolean, nullable=True)
    is_withdraw = Column(Boolean, nullable=True)


class AddressHistory(Base):
    __tablename__ = "address_history"

    id = Column(String, primary_key=True) # transaction hash
    chain = Column(String, primary_key=True) # chain id
    cate_id = Column(String, nullable=True) # call type
    time_at = Column(Integer, nullable=True)
    json = Column(JSONB, nullable=True)
