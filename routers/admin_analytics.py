from datetime import datetime, timezone
import time

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import case, distinct, func
from sqlalchemy.orm import Session

from config import FEATURE_REQUEST_ADMIN_ADDRESSES
from database import get_db
from dependencies import get_current_account
from models import Account, UsageDaily
from outbound_queue import outbound_queue


router = APIRouter(prefix="/admin/analytics", tags=["admin analytics"])


def _require_admin(
    account: Account = Depends(get_current_account),
) -> Account:
    addresses = {account.init_address.lower()}
    addresses.update(address.address.lower() for address in account.addresses)
    if not FEATURE_REQUEST_ADMIN_ADDRESSES or not (
        addresses & FEATURE_REQUEST_ADMIN_ADDRESSES
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return account


def _day_label(day: int) -> str:
    return datetime.fromtimestamp(day * 86400, tz=timezone.utc).date().isoformat()


@router.get("/queues")
async def get_admin_queue_status(
    response: Response,
    _: Account = Depends(_require_admin),
):
    response.headers["Cache-Control"] = "private, no-store"
    return await outbound_queue.status(include_activity=True)


@router.get("")
async def get_admin_analytics(
    response: Response,
    days: int = Query(7),
    _: Account = Depends(_require_admin),
    db: Session = Depends(get_db),
):
    if days not in {1, 7, 30}:
        raise HTTPException(
            status_code=400,
            detail="Supported periods: 1, 7, or 30 days",
        )
    response.headers["Cache-Control"] = "private, no-store"

    current_day = int(time.time()) // 86400
    first_day = current_day - days + 1
    period = db.query(UsageDaily).filter(UsageDaily.day >= first_day)
    total_requests = int(
        period.with_entities(
            func.coalesce(func.sum(UsageDaily.request_count), 0)
        ).scalar()
        or 0
    )
    active_users = int(
        period.with_entities(func.count(distinct(UsageDaily.account_id))).scalar() or 0
    )
    error_requests = int(
        period.with_entities(
            func.coalesce(
                func.sum(
                    case(
                        (
                            UsageDaily.status_group != "success",
                            UsageDaily.request_count,
                        ),
                        else_=0,
                    )
                ),
                0,
            )
        ).scalar()
        or 0
    )
    registered_users = int(db.query(func.count(Account.id)).scalar() or 0)

    source_rows = (
        period.with_entities(
            UsageDaily.source,
            func.sum(UsageDaily.request_count).label("requests"),
            func.count(distinct(UsageDaily.account_id)).label("users"),
            func.sum(
                case(
                    (UsageDaily.status_group != "success", UsageDaily.request_count),
                    else_=0,
                )
            ).label("errors"),
        )
        .group_by(UsageDaily.source)
        .order_by(func.sum(UsageDaily.request_count).desc())
        .limit(12)
        .all()
    )
    daily_rows = {
        day: {"requests": int(requests), "users": int(users)}
        for day, requests, users in (
            period.with_entities(
                UsageDaily.day,
                func.sum(UsageDaily.request_count),
                func.count(distinct(UsageDaily.account_id)),
            )
            .group_by(UsageDaily.day)
            .all()
        )
    }

    return {
        "period_days": days,
        "registered_users": registered_users,
        "active_users": active_users,
        "requests": total_requests,
        "errors": error_requests,
        "success_rate": (
            round((total_requests - error_requests) / total_requests * 100, 1)
            if total_requests
            else 100.0
        ),
        "daily": [
            {
                "date": _day_label(day),
                **daily_rows.get(day, {"requests": 0, "users": 0}),
            }
            for day in range(first_day, current_day + 1)
        ],
        "sources": [
            {
                "source": source,
                "requests": int(requests),
                "users": int(users),
                "errors": int(errors),
            }
            for source, requests, users, errors in source_rows
        ],
    }
