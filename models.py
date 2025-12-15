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
