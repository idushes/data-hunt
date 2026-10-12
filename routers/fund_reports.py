import json
import time
from email.utils import parseaddr
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from database import get_db
from dependencies import get_current_account
from fund_report_config import settings
from fund_reports import SOURCE_ID, require_owner, value_response, verify_webhook
from models import Account, FundReportJob, FundReportSource

router = APIRouter(tags=["private fund reports"])


@router.post("/webhooks/resend", include_in_schema=False)
async def receive_email(request: Request, db: Session = Depends(get_db)):
    if not settings.enabled:
        raise HTTPException(status_code=503, detail="Fund report import is disabled")
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 65536:
            raise HTTPException(status_code=413, detail="Webhook is too large")
    verify_webhook(bytes(body), request.headers, settings.webhook_secret)
    try:
        event = json.loads(body)
        if not isinstance(event, dict):
            raise ValueError
        if event.get("type") != "email.received":
            return {"accepted": False}
        data = event["data"]
        recipient = {parseaddr(item)[1].lower() for item in data["to"]}
        sender = parseaddr(data["from"])[1].lower()
        if settings.recipient not in recipient or sender != settings.allowed_sender:
            return {"accepted": False}
        email_id = str(UUID(data["email_id"]))
        attachments = data.get("attachments", [])
        if len(attachments) > 20:
            raise ValueError
        ids = [
            str(UUID(item["id"]))
            for item in attachments
            if item.get("content_type") == "application/pdf"
        ]
    except (KeyError, ValueError, TypeError, AttributeError):
        raise HTTPException(
            status_code=400, detail="Invalid received-email event"
        ) from None
    if not ids:
        return {"accepted": False}
    source = db.get(FundReportSource, SOURCE_ID)
    if source is None or source.account_id != settings.account_id:
        raise HTTPException(status_code=503, detail="Fund report source is not ready")
    now = int(time.time())
    for attachment_id in set(ids):
        # One savepoint per attachment: a replay cannot roll back another insertion.
        try:
            with db.begin_nested():
                db.add(
                    FundReportJob(
                        source_id=SOURCE_ID,
                        email_id=email_id,
                        attachment_id=attachment_id,
                        status="pending",
                        attempts=0,
                        created_at=now,
                        next_attempt_at=now,
                        notification_status="none",
                        notification_attempts=0,
                    )
                )
                db.flush()
        except IntegrityError:
            pass
    db.commit()
    return {"accepted": True}


@router.get("/fund-reports/value.csv", include_in_schema=False)
async def fund_value(
    request: Request,
    field: Literal["amount", "as_of"],
    db: Session = Depends(get_db),
):
    return value_response(request, db, field, settings)


@router.get("/fund-reports/status", include_in_schema=False)
async def fund_status(
    request: Request,
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db),
):
    source = require_owner(request, db, settings)
    job = (
        db.query(FundReportJob)
        .filter_by(source_id=SOURCE_ID)
        .order_by(
            FundReportJob.created_at.desc(),
            FundReportJob.id.desc(),
        )
        .first()
    )
    return {
        "source": SOURCE_ID,
        "as_of": source.as_of,
        "updated_at": source.updated_at,
        "latest_job": None
        if job is None
        else {
            "id": job.id,
            "status": job.status,
            "attempts": job.attempts,
            "error_code": job.error_code,
            "notification_status": job.notification_status,
        },
    }
