import asyncio
import base64
import hashlib
import hmac
import importlib.util
import io
import json
import logging
import tempfile
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, DecodedStreamObject
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

from csv_cache import CSVCacheMiddleware
from database import Base, get_db
from fund_report_config import FundReportSettings
from fund_reports import (
    _AttachmentLogFilter,
    FundReportWorker,
    ReportError,
    financial_block,
    read_pdf,
    verify_webhook,
)
from models import Account, AccountToken, FundReportJob, FundReportSource, ValueResource
from routers.fund_reports import router as fund_router
from routers.value import router as value_router
from scheduled_refresh import ScheduledRefreshQueue
from security import create_access_token, create_sheets_access_token
from value_rate_limit import ValueRateLimitMiddleware


def synthetic_pdf(
    password=None, amount="123,456.78", as_of="01 September 2026", blank=False
):
    writer = PdfWriter()
    page = writer.add_blank_page(612, 792)
    if not blank:
        font = DictionaryObject(
            {
                NameObject("/Type"): NameObject("/Font"),
                NameObject("/Subtype"): NameObject("/Type1"),
                NameObject("/BaseFont"): NameObject("/Helvetica"),
            }
        )
        page[NameObject("/Resources")] = DictionaryObject(
            {
                NameObject("/Font"): DictionaryObject(
                    {NameObject("/F1"): writer._add_object(font)}
                ),
            }
        )
        lines = [
            "PRIVATE PERSON, PRIVATE STREET",
            "09 September 2026",
            "LIQUID ALPHA INCOME FUND \\(LAIF\\) SP",
            f"Shareholder Value as of {as_of}",
            "Number of shares held 100.0000",
            "Net Asset Value per Share 1,234.57",
            f"Value of Shareholding USD {amount}",
            "Please contact PRIVATE CONTACT",
        ]
        stream = DecodedStreamObject()
        stream.set_data(
            (
                "BT /F1 12 Tf 40 750 Td 20 TL "
                + " ".join(f"({line}) Tj T*" for line in lines)
                + " ET"
            ).encode()
        )
        page[NameObject("/Contents")] = writer._add_object(stream)
    if password:
        writer.encrypt(password, algorithm="AES-256")
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


SECRET = "whsec_" + base64.b64encode(b"synthetic-webhook-key").decode()


def signed_headers(body, now=None):
    stamp = str(int(time.time()) if now is None else now)
    event_id = "msg_test"
    digest = hmac.new(
        b"synthetic-webhook-key",
        event_id.encode() + b"." + stamp.encode() + b"." + body,
        hashlib.sha256,
    ).digest()
    return {
        "svix-id": event_id,
        "svix-timestamp": stamp,
        "svix-signature": "v1," + base64.b64encode(digest).decode(),
    }


class FundReportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.engine = create_engine(
            "sqlite:///" + self.temp.name + "/test.db",
            connect_args={"check_same_thread": False},
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.cfg = FundReportSettings(
            enabled=True,
            account_id="owner",
            recipient="fund@unit.resend.app",
            allowed_sender="administrator@example.test",
            pdf_password="unit-test-password",
            resend_api_key="test-resend",
            webhook_secret=SECRET,
            notification_from="notifier@example.test",
            notification_to="owner@example.test",
            openrouter_api_key="test-openrouter",
        )
        with self.Session() as db:
            for account_id in ("owner", "other"):
                db.add(
                    Account(
                        id=account_id, init_address="test", init_address_network="test"
                    )
                )
                db.add(
                    AccountToken(
                        id=account_id,
                        account_id=account_id,
                        purpose="sheets",
                        is_active=True,
                        created_at=1,
                    )
                )
                db.add(
                    AccountToken(
                        id=account_id + "-login",
                        account_id=account_id,
                        purpose="session",
                        is_active=True,
                        created_at=1,
                    )
                )
            db.commit()
        self.worker = FundReportWorker(self.cfg, self.Session)
        self.worker.initialize()
        self.pdf = synthetic_pdf(self.cfg.pdf_password)
        self.requests = []
        self.model_content = {
            "amount": "123456.78",
            "currency": "USD",
            "as_of": "2026-09-01",
        }
        self.model_status = 200
        self.download_url = "https://inbound-cdn.resend.com/file?signature=private"
        self.notification_keys = set()
        self.client = httpx.AsyncClient(transport=httpx.MockTransport(self.provider))
        self.worker.client = self.client

    def tearDown(self):
        asyncio.run(self.client.aclose())
        self.engine.dispose()
        self.temp.cleanup()

    def provider(self, request):
        self.requests.append(request)
        if request.url.host == "api.resend.com" and request.method == "GET":
            return httpx.Response(
                200, json={"size": len(self.pdf), "download_url": self.download_url}
            )
        if request.url.host in {"inbound-cdn.resend.com", "cdn.resend.app"}:
            return httpx.Response(200, content=self.pdf)
        if request.url.host == "openrouter.ai":
            return httpx.Response(
                self.model_status,
                json={
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {"content": json.dumps(self.model_content)},
                        }
                    ]
                },
            )
        if request.url.host == "api.resend.com" and request.method == "POST":
            self.notification_keys.add(request.headers["Idempotency-Key"])
            return httpx.Response(200, json={"id": "test-notification"})
        raise AssertionError("Unexpected outbound request")

    def job(self):
        with self.Session() as db:
            job = FundReportJob(
                source_id="laif",
                email_id=str(uuid.uuid4()),
                attachment_id=str(uuid.uuid4()),
                created_at=int(time.time()),
                next_attempt_at=0,
            )
            db.add(job)
            db.commit()
            return job.id

    def run_job(self):
        return asyncio.run(self.worker.run_once())

    def state(self, job_id):
        with self.Session() as db:
            job = db.get(FundReportJob, job_id)
            return job.status, job.attempts, job.notification_status, job.error_code

    def app(self):
        app = FastAPI()
        app.add_middleware(CSVCacheMiddleware)
        app.add_middleware(ValueRateLimitMiddleware, session_factory=self.Session)
        app.include_router(fund_router)
        app.include_router(value_router)

        def db_override():
            with self.Session() as db:
                yield db

        app.dependency_overrides[get_db] = db_override
        return app

    def patches(self):
        from contextlib import ExitStack

        stack = ExitStack()
        stack.enter_context(patch("routers.fund_reports.settings", self.cfg))
        stack.enter_context(patch("fund_reports.settings", self.cfg))
        stack.enter_context(
            patch("value_rate_limit.get_redis_client", return_value=None)
        )
        stack.enter_context(patch("csv_cache.get_redis_client", return_value=None))
        return stack

    def test_encrypted_pdf_and_exact_financial_block(self):
        block, values = read_pdf(self.pdf, self.cfg.pdf_password)
        self.assertEqual(values, (Decimal("123456.78"), "USD", date(2026, 9, 1)))
        self.assertEqual(len(block.splitlines()), 2)
        self.assertNotIn("PRIVATE", block)
        self.assertNotIn("09 September", block)

    def test_pdf_failures(self):
        for content, password, code in [
            (self.pdf, "wrong", "wrong_pdf_password"),
            (b"%PDF-broken", "", "invalid_pdf"),
            (synthetic_pdf(blank=True), "", "pdf_has_no_text"),
            (synthetic_pdf(amount="1,23.00"), "", "ambiguous_financial_block"),
        ]:
            with self.subTest(code=code), self.assertRaises(ReportError) as exc:
                read_pdf(content, password)
            self.assertEqual(exc.exception.code, code)

    def test_ambiguous_labels_wrong_fund_and_date_rejected(self):
        text = "LIQUID ALPHA INCOME FUND (LAIF) SP\nShareholder Value as of 01 September 2026\nValue of Shareholding USD 123.45"
        for changed in (
            text + "\nValue of Shareholding USD 456.78",
            text.replace("LAIF", "OTHER"),
            text.replace("01 September", "31 September"),
        ):
            with self.assertRaises(ReportError):
                financial_block(changed)

    def test_signature_raw_body_expiry_and_multiple_versions(self):
        body = b'{"test":true}'
        headers = signed_headers(body)
        headers["svix-signature"] = "v0,old v1,wrong " + headers["svix-signature"]
        verify_webhook(body, headers, SECRET)
        for wrong_body, wrong_headers in [
            (body + b" ", headers),
            (body, signed_headers(body, int(time.time()) - 301)),
            (body, {}),
        ]:
            with self.assertRaises(HTTPException):
                verify_webhook(wrong_body, wrong_headers, SECRET)

    def test_success_and_content_privacy(self):
        job_id = self.job()
        self.assertTrue(self.run_job())
        self.assertEqual(self.state(job_id), ("succeeded", 1, "none", None))
        model = next(r for r in self.requests if r.url.host == "openrouter.ai")
        data = json.loads(model.content)
        self.assertTrue(data["provider"]["zdr"])
        self.assertEqual(data["provider"]["data_collection"], "deny")
        self.assertEqual(data["response_format"]["type"], "json_schema")
        self.assertNotIn("PRIVATE", data["messages"][1]["content"])
        with self.Session() as db:
            self.assertEqual(
                db.get(FundReportSource, "laif").amount, Decimal("123456.78")
            )
        columns = set(
            inspect(self.engine).get_columns("fund_report_job")[i]["name"]
            for i in range(len(inspect(self.engine).get_columns("fund_report_job")))
        )
        self.assertFalse(columns & {"body", "pdf", "text", "password", "download_url"})

    def test_newer_report_updates_both_values(self):
        self.job()
        self.run_job()
        self.pdf = synthetic_pdf(
            self.cfg.pdf_password, amount="456,789.01", as_of="08 September 2026"
        )
        self.model_content = {
            "amount": "456789.01",
            "currency": "USD",
            "as_of": "2026-09-08",
        }
        job_id = self.job()
        self.run_job()
        self.assertEqual(self.state(job_id)[0], "succeeded")
        with self.Session() as db:
            source = db.get(FundReportSource, "laif")
            self.assertEqual(source.amount, Decimal("456789.01"))
            self.assertEqual(source.as_of, date(2026, 9, 8))

    def test_private_previews_never_read_financial_data_from_redis(self):
        login = create_access_token({"sub": "owner", "jti": "owner-login"})
        headers = {"Authorization": "Bearer " + login}
        with self.patches(), TestClient(self.app()) as client:
            created = client.post(
                "/value-resources",
                json={"source": "fund-report", "parameters": {"field": "amount"}},
                headers=headers,
            )
            resource_id = created.json()["id"]
            copied = client.post(
                f"/value-resources/{resource_id}/copies", headers=headers
            )
            self.assertEqual(copied.status_code, 200)
            cached = AsyncMock(return_value={})
            with patch("routers.value.get_redis_client", return_value=object()), patch(
                "routers.value.load_cached_csv_previews", cached
            ):
                result = client.post(
                    "/value-resources/previews",
                    json={"resource_ids": [resource_id]},
                    headers=headers,
                )
            self.assertEqual(result.status_code, 200)
            self.assertIsNone(result.json()["items"][0]["value"])
            self.assertEqual(cached.call_args.args[1], [])
            other = create_access_token({"sub": "other", "jti": "other-login"})
            forbidden = client.post(
                "/value-resources",
                json={"source": "fund-report", "parameters": {"field": "amount"}},
                headers={"Authorization": "Bearer " + other},
            )
            self.assertEqual(forbidden.status_code, 404)

    def test_model_errors_do_not_replace_last_value(self):
        self.job()
        self.run_job()
        for model in [
            {"amount": "1.00", "currency": "USD", "as_of": "2026-09-01"},
            {"amount": "123456.78", "currency": "USD", "as_of": "2026-09-09"},
            {"amount": 123456.78, "currency": "USD", "as_of": "2026-09-01"},
            {"amount": "123456.78", "currency": "USD", "as_of": "2026-09-31"},
            {"unexpected": "bad"},
        ]:
            with self.subTest(model=model):
                self.pdf = (
                    synthetic_pdf(self.cfg.pdf_password)
                    + b"\n% "
                    + uuid.uuid4().hex.encode()
                )
                self.model_content = model
                job_id = self.job()
                self.run_job()
                self.assertEqual(self.state(job_id)[0], "failed")
                self.run_job()  # notification
        with self.Session() as db:
            self.assertEqual(
                db.get(FundReportSource, "laif").amount, Decimal("123456.78")
            )

    def test_duplicate_file_skips_model(self):
        self.job()
        self.run_job()
        job_id = self.job()
        self.run_job()
        self.assertEqual(self.state(job_id)[0], "duplicate")
        self.assertEqual(sum(r.url.host == "openrouter.ai" for r in self.requests), 1)

    def test_retry_limit_and_one_notification(self):
        self.model_status = 503
        job_id = self.job()
        for attempt in range(1, 6):
            self.run_job()
            self.assertEqual(self.state(job_id)[1], attempt)
            self.assertEqual(
                self.state(job_id)[0], "retry" if attempt < 5 else "failed"
            )
            with self.Session() as db:
                db.get(FundReportJob, job_id).next_attempt_at = 0
                db.commit()
        self.run_job()
        self.assertFalse(self.run_job())
        self.assertEqual(self.state(job_id)[2], "sent")
        self.assertEqual(len(self.notification_keys), 1)
        sent = next(
            r
            for r in self.requests
            if r.method == "POST" and r.url.host == "api.resend.com"
        )
        self.assertNotIn("123456", sent.content.decode())
        self.assertNotIn(self.cfg.pdf_password, sent.content.decode())

    def test_same_date_conflict_and_older_report(self):
        self.job()
        self.run_job()
        self.pdf = synthetic_pdf(self.cfg.pdf_password, amount="123,455.78")
        self.model_content["amount"] = "123455.78"
        job_id = self.job()
        self.run_job()
        self.assertEqual(self.state(job_id)[0], "conflict")
        self.run_job()
        self.pdf = synthetic_pdf(self.cfg.pdf_password, as_of="01 August 2026")
        self.model_content["amount"] = "123456.78"
        self.model_content["as_of"] = "2026-08-01"
        older = self.job()
        self.run_job()
        self.assertEqual(self.state(older)[0], "obsolete")
        with self.Session() as db:
            self.assertEqual(
                db.get(FundReportSource, "laif").amount, Decimal("123456.78")
            )

    def test_durable_lease_concurrency_and_restart(self):
        job_id = self.job()
        now = int(time.time())
        with ThreadPoolExecutor(2) as pool:
            claims = list(pool.map(lambda _: self.worker.claim(now), range(2)))
        self.assertEqual(sum(claim is not None for claim in claims), 1)
        restarted = FundReportWorker(self.cfg, self.Session)
        self.assertIsNone(restarted.claim(now + 1))
        reclaimed = restarted.claim(now + 301)
        self.assertEqual(reclaimed[0], job_id)
        old = next(claim for claim in claims if claim)
        self.worker.finish(
            job_id,
            old[3],
            values=(Decimal("1.00"), "USD", date(2026, 9, 1)),
            now=now + 302,
        )
        self.assertEqual(self.state(job_id)[0], "processing")
        restarted.finish(
            job_id,
            reclaimed[3],
            values=(Decimal("2.00"), "USD", date(2026, 9, 1)),
            now=now + 302,
        )
        self.assertEqual(self.state(job_id)[0], "succeeded")

    def test_notification_recovery_uses_same_idempotency_key(self):
        self.pdf = b"%PDF-broken"
        job_id = self.job()
        self.run_job()
        claim = self.worker.claim(int(time.time()))
        original_owned = self.worker.owned
        calls = 0

        def lose_lease(db, token, now):
            nonlocal calls
            calls += 1
            return original_owned(db, token, now) if calls == 1 else None

        with patch.object(self.worker, "owned", side_effect=lose_lease):
            asyncio.run(self.worker.notify(self.client, job_id, claim[3]))
        with self.Session() as db:
            db.get(FundReportSource, "laif").lease_until = 0
            db.commit()
        self.run_job()
        self.assertEqual(len(self.notification_keys), 1)
        self.assertEqual(self.state(job_id)[2], "sent")

    def test_notification_not_resent_after_idempotency_window(self):
        self.pdf = b"%PDF-broken"
        job_id = self.job()
        self.run_job()
        with self.Session() as db:
            db.get(FundReportJob, job_id).notification_started_at = (
                int(time.time()) - 86400
            )
            db.commit()
        self.run_job()
        self.assertEqual(self.state(job_id)[2], "unknown")
        self.assertEqual(len(self.notification_keys), 0)

    def test_current_resend_cdn_download(self):
        self.download_url = "https://cdn.resend.app/file?signature=private"
        job_id = self.job()
        self.run_job()
        self.assertEqual(self.state(job_id)[0], "succeeded")

    def test_signed_download_urls_are_not_logged(self):
        filter_ = _AttachmentLogFilter()
        for host in ("cdn.resend.app", "inbound-cdn.resend.com"):
            record = logging.LogRecord(
                "httpx",
                logging.INFO,
                "",
                1,
                "GET https://%s/file?signature=secret",
                (host,),
                None,
            )
            self.assertFalse(filter_.filter(record))

    def test_attachment_ssrf_blocked(self):
        for url in [
            "https://localhost/private",
            "http://inbound-cdn.resend.com/file",
            "https://inbound-cdn.resend.com:8000/file",
            "https://inbound-cdn.resend.com@evil.test/file",
            "https://cdn.resend.app.evil.test/file",
            "http://cdn.resend.app/file",
        ]:
            with self.subTest(url=url):
                self.download_url = url
                job_id = self.job()
                self.run_job()
                self.assertEqual(self.state(job_id)[3], "invalid_attachment_metadata")
                self.run_job()

    def test_webhook_filters_signature_and_attachment_replay(self):
        event = {
            "type": "email.received",
            "data": {
                "email_id": str(uuid.uuid4()),
                "from": self.cfg.allowed_sender,
                "to": [self.cfg.recipient],
                "attachments": [
                    {"id": str(uuid.uuid4()), "content_type": "application/pdf"}
                ],
            },
        }
        with self.patches(), TestClient(self.app()) as client:
            body = json.dumps(event).encode()
            self.assertEqual(
                client.post("/webhooks/resend", content=body).status_code, 401
            )
            for _ in range(2):
                self.assertEqual(
                    client.post(
                        "/webhooks/resend", content=body, headers=signed_headers(body)
                    ).json(),
                    {"accepted": True},
                )
            for field, value in [
                ("from", "other@example.test"),
                ("to", ["other@example.test"]),
            ]:
                changed = json.loads(body)
                changed["data"][field] = value
                rejected = json.dumps(changed).encode()
                self.assertEqual(
                    client.post(
                        "/webhooks/resend",
                        content=rejected,
                        headers=signed_headers(rejected),
                    ).json(),
                    {"accepted": False},
                )
            huge = b" " * 65537
            self.assertEqual(
                client.post(
                    "/webhooks/resend", content=huge, headers=signed_headers(huge)
                ).status_code,
                413,
            )
        with self.Session() as db:
            self.assertEqual(db.query(FundReportJob).count(), 1)

    def test_private_urls_owner_revocation_and_no_cache(self):
        self.job()
        self.run_job()
        owner = create_sheets_access_token({"sub": "owner", "jti": "owner"})
        other = create_sheets_access_token({"sub": "other", "jti": "other"})
        login = create_access_token({"sub": "owner", "jti": "owner-login"})
        with self.patches(), TestClient(self.app()) as client:
            for field, expected in [("amount", "123456.78"), ("as_of", "2026-09-01")]:
                created = client.post(
                    "/value-resources",
                    json={"source": "fund-report", "parameters": {"field": field}},
                    headers={"Authorization": "Bearer " + login},
                )
                self.assertEqual(created.status_code, 200, created.text)
                short = "/v/" + created.json()["id"]
                for url in [short, f"/fund-reports/value.csv?field={field}"]:
                    owner_url = (
                        url + ("&" if "?" in url else "?") + "auth_token=" + owner
                    )
                    other_url = (
                        url + ("&" if "?" in url else "?") + "auth_token=" + other
                    )
                    self.assertEqual(client.get(url).status_code, 401)
                    for _ in range(2):
                        response = client.get(owner_url)
                        self.assertEqual(response.status_code, 200, response.text)
                        self.assertEqual(response.text, expected)
                        self.assertEqual(
                            response.headers["cache-control"], "private, no-store"
                        )
                        self.assertNotIn("x-csv-cache", response.headers)
                    self.assertEqual(client.get(other_url).status_code, 404)
                with self.Session() as db:
                    db.get(FundReportSource, "laif").amount = Decimal("42.00")
                    db.commit()
                if field == "amount":
                    self.assertEqual(
                        client.get(short, params={"auth_token": owner}).text, "42.00"
                    )
            status = client.get(
                "/fund-reports/status", headers={"Authorization": "Bearer " + login}
            )
            self.assertEqual(status.status_code, 200)
            self.assertEqual(status.json()["as_of"], "2026-09-01")
            self.assertEqual(
                client.get(
                    "/fund-reports/status", params={"auth_token": owner}
                ).status_code,
                401,
            )
            with self.Session() as db:
                db.get(AccountToken, "owner").is_active = False
                db.commit()
            self.assertEqual(
                client.get(short, params={"auth_token": owner}).status_code, 401
            )

    def test_private_source_not_scheduled(self):
        with self.Session() as db:
            db.add(
                ValueResource(
                    id="AbCdEf123456",
                    fingerprint="test",
                    source="fund-report",
                    parameters={"field": "amount"},
                    created_at=1,
                )
            )
            db.commit()
        queue = ScheduledRefreshQueue(
            session_factory=self.Session, redis_client=object()
        )
        self.assertFalse(asyncio.run(queue.schedule("AbCdEf123456")))

    def test_disabled_import_and_owner_change(self):
        with patch(
            "routers.fund_reports.settings", replace(self.cfg, enabled=False)
        ), TestClient(self.app()) as client:
            self.assertEqual(
                client.post("/webhooks/resend", content=b"{}").status_code, 503
            )
        with self.assertRaises(ValueError):
            FundReportWorker(
                replace(self.cfg, account_id="other"), self.Session
            ).initialize()


class FundReportMigrationTest(unittest.TestCase):
    def test_upgrade_downgrade(self):
        engine = create_engine("sqlite://")
        Account.__table__.create(engine)
        path = (
            Path(__file__).resolve().parents[1]
            / "alembic/versions/72a1ce490bf6_add_private_fund_reports.py"
        )
        spec = importlib.util.spec_from_file_location("fund_report_migration", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with engine.begin() as conn:
            with Operations.context(MigrationContext.configure(conn)):
                module.upgrade()
                self.assertIn("fund_report_job", inspect(conn).get_table_names())
                self.assertEqual(
                    len(inspect(conn).get_unique_constraints("fund_report_job")), 2
                )
                module.downgrade()
                self.assertEqual(inspect(conn).get_table_names(), ["account"])
        engine.dispose()
