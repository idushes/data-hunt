import uuid
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
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


class FeatureRequest(Base):
    __tablename__ = "feature_request"

    id = Column(Integer, primary_key=True)
    title = Column(String(120), nullable=False)
    normalized_title = Column(String(120), nullable=False, unique=True, index=True)
    description = Column(Text, nullable=True)
    category = Column(String(32), nullable=False, index=True)
    status = Column(String(32), nullable=False, default="requested", index=True)
    created_by_account_id = Column(
        String,
        ForeignKey("account.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at = Column(Integer, nullable=False)
    updated_at = Column(Integer, nullable=False)

    creator = relationship("Account")
    votes = relationship(
        "FeatureRequestVote",
        back_populates="feature_request",
        cascade="all, delete-orphan",
    )
    feedback = relationship(
        "FeatureRequestFeedback",
        back_populates="feature_request",
        cascade="all, delete-orphan",
    )


class FeatureRequestVote(Base):
    __tablename__ = "feature_request_vote"
    __table_args__ = (
        UniqueConstraint(
            "feature_request_id",
            "account_id",
            name="uq_feature_request_vote_request_account",
        ),
    )

    id = Column(Integer, primary_key=True)
    feature_request_id = Column(
        Integer,
        ForeignKey("feature_request.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    account_id = Column(
        String,
        ForeignKey("account.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at = Column(Integer, nullable=False)

    feature_request = relationship("FeatureRequest", back_populates="votes")
    account = relationship("Account")


class FeatureRequestFeedback(Base):
    __tablename__ = "feature_request_feedback"
    __table_args__ = (
        UniqueConstraint(
            "feature_request_id",
            "account_id",
            name="uq_feature_request_feedback_request_account",
        ),
    )

    id = Column(Integer, primary_key=True)
    feature_request_id = Column(
        Integer,
        ForeignKey("feature_request.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    account_id = Column(
        String,
        ForeignKey("account.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    verdict = Column(String(24), nullable=False, index=True)
    comment = Column(Text, nullable=True)
    created_at = Column(Integer, nullable=False)
    updated_at = Column(Integer, nullable=False)

    feature_request = relationship("FeatureRequest", back_populates="feedback")
    account = relationship("Account")
