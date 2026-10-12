"""Single private fund source. Values come from server secrets, never URLs."""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class FundReportSettings:
    enabled: bool = False
    account_id: str = ""
    recipient: str = ""
    allowed_sender: str = ""
    pdf_password: str = field(default="", repr=False)
    resend_api_key: str = field(default="", repr=False)
    webhook_secret: str = field(default="", repr=False)
    notification_from: str = ""
    notification_to: str = ""
    openrouter_api_key: str = field(default="", repr=False)
    model: str = "google/gemini-3.1-flash-lite"
    poll_seconds: float = 5

    def validate(self) -> None:
        required = (
            self.account_id,
            self.recipient,
            self.allowed_sender,
            self.pdf_password,
            self.resend_api_key,
            self.webhook_secret,
            self.notification_from,
            self.notification_to,
            self.openrouter_api_key,
        )
        if self.enabled and not all(required):
            raise ValueError(
                "Enabled fund reports require all server configuration fields"
            )


settings = FundReportSettings(
    enabled=os.getenv("FUND_REPORTS_ENABLED", "false").lower() in {"true", "1", "yes"},
    account_id=os.getenv("FUND_REPORT_ACCOUNT_ID", "").strip(),
    recipient=os.getenv("FUND_REPORT_RECIPIENT", "").strip().lower(),
    allowed_sender=os.getenv("FUND_REPORT_ALLOWED_SENDER", "").strip().lower(),
    pdf_password=os.getenv("FUND_REPORT_PDF_PASSWORD", ""),
    resend_api_key=os.getenv("FUND_REPORT_RESEND_API_KEY", ""),
    webhook_secret=os.getenv("FUND_REPORT_WEBHOOK_SECRET", ""),
    notification_from=os.getenv("FUND_REPORT_NOTIFICATION_FROM", "").strip(),
    notification_to=os.getenv("FUND_REPORT_NOTIFICATION_TO", "").strip(),
    openrouter_api_key=os.getenv("FUND_REPORT_OPENROUTER_API_KEY", ""),
    model=os.getenv("FUND_REPORT_MODEL", "google/gemini-3.1-flash-lite"),
)
