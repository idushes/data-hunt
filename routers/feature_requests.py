import re
import time
from difflib import SequenceMatcher

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from config import FEATURE_REQUEST_ADMIN_ADDRESSES
from database import get_db
from dependencies import get_current_account, get_optional_current_account
from models import (
    Account,
    FeatureRequest,
    FeatureRequestFeedback,
    FeatureRequestVote,
)


router = APIRouter(prefix="/feature-requests", tags=["feature requests"])

CATEGORIES = {
    "blockchain",
    "defi_protocol",
    "exchange",
    "data_field",
    "other",
}
STATUSES = {"requested", "planned", "in_progress", "released"}
VERDICTS = {"works", "not_working"}


class CreateFeatureRequest(BaseModel):
    title: str = Field(min_length=3, max_length=120)
    description: str | None = Field(default=None, max_length=2000)
    category: str


class FeatureRequestVoteInput(BaseModel):
    active: bool


class FeatureRequestFeedbackInput(BaseModel):
    verdict: str
    comment: str | None = Field(default=None, max_length=1000)


class FeatureRequestStatusInput(BaseModel):
    status: str


def _now() -> int:
    return int(time.time())


def _normalize_title(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", value.lower()))


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned or None


def _get_request_or_404(db: Session, request_id: int) -> FeatureRequest:
    item = db.query(FeatureRequest).filter(FeatureRequest.id == request_id).first()
    if item is None:
        raise HTTPException(status_code=404, detail="Feature request not found")
    return item


def _is_admin(account: Account | None) -> bool:
    if account is None or not FEATURE_REQUEST_ADMIN_ADDRESSES:
        return False
    addresses = {account.init_address.lower()}
    addresses.update(address.address.lower() for address in account.addresses)
    return bool(addresses & FEATURE_REQUEST_ADMIN_ADDRESSES)


def _request_counts(db: Session, request_ids: list[int]):
    support = dict(
        db.query(
            FeatureRequestVote.feature_request_id,
            func.count(FeatureRequestVote.id),
        )
        .filter(FeatureRequestVote.feature_request_id.in_(request_ids))
        .group_by(FeatureRequestVote.feature_request_id)
        .all()
    ) if request_ids else {}

    feedback_rows = (
        db.query(
            FeatureRequestFeedback.feature_request_id,
            FeatureRequestFeedback.verdict,
            func.count(FeatureRequestFeedback.id),
        )
        .filter(FeatureRequestFeedback.feature_request_id.in_(request_ids))
        .group_by(
            FeatureRequestFeedback.feature_request_id,
            FeatureRequestFeedback.verdict,
        )
        .all()
        if request_ids
        else []
    )
    feedback = {
        request_id: {"works": 0, "not_working": 0}
        for request_id in request_ids
    }
    for request_id, verdict, count in feedback_rows:
        feedback[request_id][verdict] = count
    return support, feedback


def _serialize_requests(
    db: Session,
    items: list[FeatureRequest],
    viewer: Account | None,
) -> list[dict[str, object]]:
    request_ids = [item.id for item in items]
    support_counts, feedback_counts = _request_counts(db, request_ids)
    viewer_votes: set[int] = set()
    viewer_feedback: dict[int, FeatureRequestFeedback] = {}
    if viewer is not None and request_ids:
        viewer_votes = {
            request_id
            for (request_id,) in db.query(FeatureRequestVote.feature_request_id)
            .filter(
                FeatureRequestVote.account_id == viewer.id,
                FeatureRequestVote.feature_request_id.in_(request_ids),
            )
            .all()
        }
        viewer_feedback = {
            item.feature_request_id: item
            for item in db.query(FeatureRequestFeedback)
            .filter(
                FeatureRequestFeedback.account_id == viewer.id,
                FeatureRequestFeedback.feature_request_id.in_(request_ids),
            )
            .all()
        }

    result = []
    for item in items:
        counts = feedback_counts.get(item.id, {"works": 0, "not_working": 0})
        current_feedback = viewer_feedback.get(item.id)
        result.append(
            {
                "id": item.id,
                "title": item.title,
                "description": item.description or "",
                "category": item.category,
                "status": item.status,
                "created_at": item.created_at,
                "updated_at": item.updated_at,
                "support_count": support_counts.get(item.id, 0),
                "works_count": counts["works"],
                "not_working_count": counts["not_working"],
                "viewer_supports": item.id in viewer_votes,
                "viewer_feedback": (
                    {
                        "verdict": current_feedback.verdict,
                        "comment": current_feedback.comment or "",
                    }
                    if current_feedback is not None
                    else None
                ),
            }
        )
    return result


def _similarity(query: str, item: FeatureRequest) -> float:
    title = item.normalized_title
    if query == title:
        return 1.0
    if query in title or title in query:
        return 0.92
    query_tokens = set(query.split())
    title_tokens = set(title.split())
    overlap = len(query_tokens & title_tokens) / max(len(query_tokens | title_tokens), 1)
    return max(SequenceMatcher(None, query, title).ratio(), overlap * 0.9)


@router.get("")
async def list_feature_requests(
    query: str | None = Query(None, max_length=120),
    category: str | None = Query(None),
    request_status: str | None = Query(None, alias="status"),
    sort: str = Query("popular"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    viewer: Account | None = Depends(get_optional_current_account),
    db: Session = Depends(get_db),
):
    if category is not None and category not in CATEGORIES:
        raise HTTPException(status_code=400, detail="Unsupported category")
    if request_status is not None and request_status not in STATUSES:
        raise HTTPException(status_code=400, detail="Unsupported status")
    if sort not in {"popular", "newest", "oldest"}:
        raise HTTPException(status_code=400, detail="Unsupported sort")

    db_query = db.query(FeatureRequest)
    if query and query.strip():
        pattern = f"%{query.strip()}%"
        db_query = db_query.filter(
            FeatureRequest.title.ilike(pattern)
            | FeatureRequest.description.ilike(pattern)
        )
    if category:
        db_query = db_query.filter(FeatureRequest.category == category)
    if request_status:
        db_query = db_query.filter(FeatureRequest.status == request_status)

    items = db_query.all()
    serialized = _serialize_requests(db, items, viewer)
    if sort == "popular":
        serialized.sort(
            key=lambda item: (item["support_count"], item["created_at"]),
            reverse=True,
        )
    elif sort == "newest":
        serialized.sort(key=lambda item: item["created_at"], reverse=True)
    else:
        serialized.sort(key=lambda item: item["created_at"])

    total = len(serialized)
    return {
        "items": serialized[offset : offset + limit],
        "total": total,
        "viewer_is_admin": _is_admin(viewer),
    }


@router.get("/search")
async def search_similar_feature_requests(
    query: str = Query(..., min_length=2, max_length=120),
    limit: int = Query(6, ge=1, le=20),
    viewer: Account | None = Depends(get_optional_current_account),
    db: Session = Depends(get_db),
):
    normalized_query = _normalize_title(query)
    if len(normalized_query) < 2:
        return {"items": []}
    candidates = db.query(FeatureRequest).order_by(FeatureRequest.created_at.desc()).limit(1000).all()
    matches = [
        (item, _similarity(normalized_query, item))
        for item in candidates
    ]
    matches = [match for match in matches if match[1] >= 0.35]
    matches.sort(key=lambda match: (match[1], match[0].created_at), reverse=True)
    selected = matches[:limit]
    serialized = _serialize_requests(db, [item for item, _ in selected], viewer)
    for item, (_, score) in zip(serialized, selected, strict=False):
        item["match_score"] = round(score, 3)
    return {"items": serialized}


@router.get("/{request_id}")
async def get_feature_request(
    request_id: int,
    viewer: Account | None = Depends(get_optional_current_account),
    db: Session = Depends(get_db),
):
    item = _get_request_or_404(db, request_id)
    return {
        "item": _serialize_requests(db, [item], viewer)[0],
        "viewer_is_admin": _is_admin(viewer),
    }


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_feature_request(
    payload: CreateFeatureRequest,
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db),
):
    title = " ".join(payload.title.split())
    normalized_title = _normalize_title(title)
    if len(normalized_title) < 3:
        raise HTTPException(status_code=400, detail="Title is too short")
    if payload.category not in CATEGORIES:
        raise HTTPException(status_code=400, detail="Unsupported category")

    existing = db.query(FeatureRequest).filter(
        FeatureRequest.normalized_title == normalized_title
    ).first()
    if existing is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "A feature request with this title already exists",
                "request_id": existing.id,
            },
        )

    timestamp = _now()
    item = FeatureRequest(
        title=title,
        normalized_title=normalized_title,
        description=_clean_text(payload.description),
        category=payload.category,
        status="requested",
        created_by_account_id=account.id,
        created_at=timestamp,
        updated_at=timestamp,
    )
    db.add(item)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        duplicate = db.query(FeatureRequest).filter(
            FeatureRequest.normalized_title == normalized_title
        ).first()
        raise HTTPException(
            status_code=409,
            detail={
                "message": "A feature request with this title already exists",
                "request_id": duplicate.id if duplicate else None,
            },
        ) from exc
    db.refresh(item)
    return {"item": _serialize_requests(db, [item], account)[0]}


@router.put("/{request_id}/vote")
async def set_feature_request_vote(
    request_id: int,
    payload: FeatureRequestVoteInput,
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db),
):
    item = _get_request_or_404(db, request_id)
    vote = db.query(FeatureRequestVote).filter(
        FeatureRequestVote.feature_request_id == request_id,
        FeatureRequestVote.account_id == account.id,
    ).first()
    if payload.active and vote is None:
        db.add(
            FeatureRequestVote(
                feature_request_id=request_id,
                account_id=account.id,
                created_at=_now(),
            )
        )
    elif not payload.active and vote is not None:
        db.delete(vote)
    try:
        db.commit()
    except IntegrityError:
        # Two rapid identical vote requests can race against the unique index.
        # The desired end state is still a single active vote.
        db.rollback()
        if not payload.active:
            existing_vote = db.query(FeatureRequestVote).filter(
                FeatureRequestVote.feature_request_id == request_id,
                FeatureRequestVote.account_id == account.id,
            ).first()
            if existing_vote is not None:
                db.delete(existing_vote)
                db.commit()
        item = _get_request_or_404(db, request_id)
    return {"item": _serialize_requests(db, [item], account)[0]}


@router.get("/{request_id}/feedback")
async def list_feature_request_feedback(
    request_id: int,
    db: Session = Depends(get_db),
):
    _get_request_or_404(db, request_id)
    entries = db.query(FeatureRequestFeedback).filter(
        FeatureRequestFeedback.feature_request_id == request_id
    ).order_by(FeatureRequestFeedback.updated_at.desc()).all()
    return {
        "items": [
            {
                "id": entry.id,
                "verdict": entry.verdict,
                "comment": entry.comment or "",
                "author_address": entry.account.init_address,
                "updated_at": entry.updated_at,
            }
            for entry in entries
        ]
    }


@router.put("/{request_id}/feedback")
async def set_feature_request_feedback(
    request_id: int,
    payload: FeatureRequestFeedbackInput,
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db),
):
    item = _get_request_or_404(db, request_id)
    if item.status != "released":
        raise HTTPException(
            status_code=409,
            detail="Feedback is available after the feature is released",
        )
    if payload.verdict not in VERDICTS:
        raise HTTPException(status_code=400, detail="Unsupported verdict")

    timestamp = _now()
    feedback = db.query(FeatureRequestFeedback).filter(
        FeatureRequestFeedback.feature_request_id == request_id,
        FeatureRequestFeedback.account_id == account.id,
    ).first()
    if feedback is None:
        feedback = FeatureRequestFeedback(
            feature_request_id=request_id,
            account_id=account.id,
            verdict=payload.verdict,
            comment=_clean_text(payload.comment),
            created_at=timestamp,
            updated_at=timestamp,
        )
        db.add(feedback)
    else:
        feedback.verdict = payload.verdict
        feedback.comment = _clean_text(payload.comment)
        feedback.updated_at = timestamp
    db.commit()
    return {"item": _serialize_requests(db, [item], account)[0]}


@router.patch("/{request_id}/status")
async def update_feature_request_status(
    request_id: int,
    payload: FeatureRequestStatusInput,
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db),
):
    if not _is_admin(account):
        raise HTTPException(status_code=403, detail="Administrator access required")
    if payload.status not in STATUSES:
        raise HTTPException(status_code=400, detail="Unsupported status")

    item = _get_request_or_404(db, request_id)
    item.status = payload.status
    item.updated_at = _now()
    db.commit()
    return {"item": _serialize_requests(db, [item], account)[0]}
