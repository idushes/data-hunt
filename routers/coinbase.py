import csv
import io
import os
import secrets
import time
from decimal import Decimal, InvalidOperation
from urllib.parse import parse_qsl, urlparse

import httpx
import jwt
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response


COINBASE_API_BASE_URL = "https://api.coinbase.com"
COINBASE_API_HOST = "api.coinbase.com"
COINBASE_ACCOUNTS_PATH = "/v2/accounts"
COINBASE_ACCOUNTS_LIMIT = "100"
COINBASE_MAX_PAGES = 100
COINBASE_API_KEY_NAME_ENV = "COINBASE_API_KEY_NAME"
COINBASE_API_KEY_SECRET_ENV = "COINBASE_API_KEY_SECRET"

COINBASE_CSV_HEADER = [
    "id",
    "name",
    "currency",
    "currency_name",
    "currency_type",
    "balance",
    "balance_currency",
    "account_type",
    "primary",
    "ready",
    "resource",
    "resource_path",
    "created_at",
    "updated_at",
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


def _csv_value(value: object | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _nested_value(item: dict[str, object], field: str, child: str) -> object | None:
    value = item.get(field)
    if isinstance(value, dict):
        return value.get(child)
    return None


def _balance_amount(account: dict[str, object]) -> object | None:
    return _nested_value(account, "balance", "amount")


def _has_positive_balance(account: dict[str, object]) -> bool:
    return _decimal_or_zero(_balance_amount(account)) > 0


def _normalize_private_key(key_secret: str) -> str:
    normalized = key_secret.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="Coinbase key_secret is required")

    normalized = normalized.replace("\\n", "\n")
    if not normalized.startswith("-----BEGIN"):
        raise HTTPException(
            status_code=400,
            detail="Coinbase key_secret must be an EC private key PEM",
        )

    return normalized


def _build_coinbase_jwt(
    key_name: str,
    key_secret: str,
    method: str = "GET",
    path: str = COINBASE_ACCOUNTS_PATH,
) -> str:
    normalized_key_name = key_name.strip()
    if not normalized_key_name:
        raise HTTPException(status_code=400, detail="Coinbase key_name is required")

    now = int(time.time())
    uri = f"{method.upper()} {COINBASE_API_HOST}{path}"

    try:
        return jwt.encode(
            {
                "sub": normalized_key_name,
                "iss": "cdp",
                "nbf": now,
                "exp": now + 120,
                "uri": uri,
            },
            _normalize_private_key(key_secret),
            algorithm="ES256",
            headers={"kid": normalized_key_name, "nonce": secrets.token_hex()},
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to sign Coinbase JWT: {exc}",
        )


def _build_auth_header(
    token: str | None,
    key_name: str | None,
    key_secret: str | None,
) -> dict[str, str]:
    if token is not None:
        normalized_token = token.strip()
        if not normalized_token:
            raise HTTPException(status_code=400, detail="Coinbase token is required")
        if not normalized_token.lower().startswith("bearer "):
            normalized_token = f"Bearer {normalized_token}"
        return {"Authorization": normalized_token, "Accept": "application/json"}

    resolved_key_name = key_name or os.getenv(COINBASE_API_KEY_NAME_ENV)
    resolved_key_secret = key_secret or os.getenv(COINBASE_API_KEY_SECRET_ENV)

    if resolved_key_name and resolved_key_secret:
        jwt_token = _build_coinbase_jwt(resolved_key_name, resolved_key_secret)
        return {"Authorization": f"Bearer {jwt_token}", "Accept": "application/json"}

    raise HTTPException(
        status_code=400,
        detail=(
            "Provide Coinbase token, key_name and key_secret, or set "
            f"{COINBASE_API_KEY_NAME_ENV}/{COINBASE_API_KEY_SECRET_ENV}"
        ),
    )


def _next_page_params(next_uri: object | None) -> dict[str, str] | None:
    if not isinstance(next_uri, str) or not next_uri:
        return None

    parsed = urlparse(next_uri)
    if parsed.path and parsed.path != COINBASE_ACCOUNTS_PATH:
        raise HTTPException(
            status_code=502, detail="Unexpected Coinbase pagination path"
        )

    return dict(parse_qsl(parsed.query, keep_blank_values=False))


async def _fetch_coinbase_json(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    params: dict[str, str],
) -> dict[str, object]:
    response = await client.get(
        f"{COINBASE_API_BASE_URL}{COINBASE_ACCOUNTS_PATH}",
        headers=headers,
        params=params,
    )

    if response.status_code == 401:
        raise HTTPException(status_code=401, detail="Invalid Coinbase credentials")

    if response.status_code >= 400:
        try:
            error_payload = response.json()
            error_message = (
                error_payload.get("message")
                or error_payload.get("error_description")
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
    client: httpx.AsyncClient,
    headers: dict[str, str],
) -> list[dict[str, object]]:
    accounts: list[dict[str, object]] = []
    params = {"limit": COINBASE_ACCOUNTS_LIMIT}

    for _ in range(COINBASE_MAX_PAGES):
        payload = await _fetch_coinbase_json(client, headers, params)
        page_accounts = payload.get("data", [])
        if not isinstance(page_accounts, list):
            raise HTTPException(
                status_code=502, detail="Unexpected Coinbase response format"
            )

        accounts.extend(item for item in page_accounts if isinstance(item, dict))

        pagination = payload.get("pagination", {})
        if not isinstance(pagination, dict):
            raise HTTPException(
                status_code=502, detail="Unexpected Coinbase response format"
            )

        next_params = _next_page_params(pagination.get("next_uri"))
        if not next_params:
            return accounts

        params = next_params

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
                _csv_value(account.get("id")),
                _csv_value(account.get("name")),
                _csv_value(_nested_value(account, "currency", "code")),
                _csv_value(_nested_value(account, "currency", "name")),
                _csv_value(_nested_value(account, "currency", "type")),
                _normalize_number(_balance_amount(account)),
                _csv_value(_nested_value(account, "balance", "currency")),
                _csv_value(account.get("type")),
                _csv_value(account.get("primary")),
                _csv_value(account.get("ready")),
                _csv_value(account.get("resource")),
                _csv_value(account.get("resource_path")),
                _csv_value(account.get("created_at")),
                _csv_value(account.get("updated_at")),
            ]
        )

    return output.getvalue()


@router.get(
    "/balance",
    summary="Export Coinbase balances for Google Sheets",
    description=(
        "Returns a CSV table with Coinbase App account balances from /v2/accounts. "
        "Pass either a ready Bearer token, key_name/key_secret from a downloaded "
        "Coinbase API key, or configure Coinbase credentials in environment variables."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_coinbase_balance(
    token: str | None = Query(
        None,
        description="Optional ready Coinbase Bearer token.",
    ),
    key_name: str | None = Query(
        None,
        description="Coinbase API key name from the downloaded API key file.",
    ),
    key_secret: str | None = Query(
        None,
        description="Coinbase EC private key from the downloaded API key file.",
    ),
    include_zero: bool = Query(
        False,
        description="Include Coinbase accounts with zero balance.",
    ),
):
    headers = _build_auth_header(token, key_name, key_secret)

    async with httpx.AsyncClient(timeout=20.0) as client:
        accounts = await _fetch_coinbase_accounts(client, headers)

    return Response(
        content=_render_coinbase_csv(accounts, include_zero),
        media_type="text/csv",
    )
