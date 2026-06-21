import csv
import io
from decimal import Decimal, InvalidOperation

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response


COINBASE_API_BASE_URL = "https://api.coinbase.com"
COINBASE_ACCOUNTS_PATH = "/api/v3/brokerage/accounts"
COINBASE_ACCOUNTS_LIMIT = "250"
COINBASE_MAX_PAGES = 100

COINBASE_CSV_HEADER = [
    "uuid",
    "name",
    "currency",
    "available_balance",
    "hold",
    "total_balance",
    "type",
    "active",
    "ready",
    "default",
    "retail_portfolio_id",
    "platform",
    "created_at",
    "updated_at",
    "deleted_at",
]

router = APIRouter(prefix="/coinbase", tags=["coinbase"])


def _normalize_number(value: object | None) -> str:
    if value is None:
        return "0"

    try:
        normalized = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return str(value)

    return format(normalized, "f")


def _decimal_or_zero(value: object | None) -> Decimal:
    if value is None:
        return Decimal("0")

    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _build_auth_header(token: str) -> dict[str, str]:
    token = token.strip()
    if not token:
        raise HTTPException(status_code=400, detail="Token is required")

    if not token.lower().startswith("bearer "):
        token = f"Bearer {token}"

    return {"Authorization": token, "Accept": "application/json"}


def _csv_value(value: object | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _balance_value(account: dict[str, object], field: str) -> object | None:
    balance = account.get(field)
    if isinstance(balance, dict):
        return balance.get("value")
    return balance


def _account_total_balance(account: dict[str, object]) -> Decimal:
    return _decimal_or_zero(_balance_value(account, "available_balance")) + (
        _decimal_or_zero(_balance_value(account, "hold"))
    )


def _has_positive_balance(account: dict[str, object]) -> bool:
    return _account_total_balance(account) > 0


async def _fetch_coinbase_json(
    client: httpx.AsyncClient,
    token: str,
    params: dict[str, str],
) -> dict[str, object]:
    response = await client.get(
        f"{COINBASE_API_BASE_URL}{COINBASE_ACCOUNTS_PATH}",
        headers=_build_auth_header(token),
        params=params,
    )

    if response.status_code == 401:
        raise HTTPException(status_code=401, detail="Invalid Coinbase token")

    if response.status_code >= 400:
        try:
            error_payload = response.json()
            error_message = (
                error_payload.get("message")
                or error_payload.get("error_details")
                or error_payload.get("error")
                or response.text
            )
        except Exception:
            error_message = response.text

        raise HTTPException(
            status_code=response.status_code,
            detail=f"Coinbase API error: {error_message}",
        )

    try:
        payload = response.json()
    except Exception:
        raise HTTPException(status_code=502, detail="Coinbase returned invalid JSON")

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502, detail="Unexpected Coinbase response format"
        )

    return payload


async def _fetch_coinbase_accounts(
    client: httpx.AsyncClient, token: str
) -> list[dict[str, object]]:
    accounts: list[dict[str, object]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()

    for _ in range(COINBASE_MAX_PAGES):
        params = {"limit": COINBASE_ACCOUNTS_LIMIT}
        if cursor:
            params["cursor"] = cursor

        payload = await _fetch_coinbase_json(client, token, params)
        page_accounts = payload.get("accounts", [])
        if not isinstance(page_accounts, list):
            raise HTTPException(
                status_code=502, detail="Unexpected Coinbase response format"
            )

        accounts.extend(
            item for item in page_accounts if isinstance(item, dict)
        )

        has_next = bool(payload.get("has_next"))
        if not has_next:
            return accounts

        next_cursor = str(payload.get("cursor") or "").strip()
        if not next_cursor or next_cursor in seen_cursors:
            raise HTTPException(
                status_code=502, detail="Invalid Coinbase pagination cursor"
            )

        seen_cursors.add(next_cursor)
        cursor = next_cursor

    raise HTTPException(status_code=502, detail="Coinbase pagination limit exceeded")


def _render_coinbase_csv(
    accounts: list[dict[str, object]], include_zero: bool
) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(COINBASE_CSV_HEADER)

    for account in accounts:
        if not include_zero and not _has_positive_balance(account):
            continue

        writer.writerow(
            [
                _csv_value(account.get("uuid")),
                _csv_value(account.get("name")),
                _csv_value(account.get("currency")),
                _normalize_number(_balance_value(account, "available_balance")),
                _normalize_number(_balance_value(account, "hold")),
                _normalize_number(_account_total_balance(account)),
                _csv_value(account.get("type")),
                _csv_value(account.get("active")),
                _csv_value(account.get("ready")),
                _csv_value(account.get("default")),
                _csv_value(account.get("retail_portfolio_id")),
                _csv_value(account.get("platform")),
                _csv_value(account.get("created_at")),
                _csv_value(account.get("updated_at")),
                _csv_value(account.get("deleted_at")),
            ]
        )

    return output.getvalue()


@router.get(
    "/balance",
    summary="Export Coinbase balances for Google Sheets",
    description=(
        "Returns a CSV table with Coinbase Advanced Trade accounts and balances. "
        "Token is accepted via query parameter for Google Sheets compatibility."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_coinbase_balance(
    token: str = Query(
        ..., description="Coinbase Bearer token. Can be passed with or without prefix."
    ),
    include_zero: bool = Query(
        False,
        description="Include Coinbase accounts with zero total balance.",
    ),
):
    async with httpx.AsyncClient(timeout=20.0) as client:
        accounts = await _fetch_coinbase_accounts(client, token)

    return Response(
        content=_render_coinbase_csv(accounts, include_zero),
        media_type="text/csv",
    )
