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
COINBASE_PORTFOLIOS_PATH = "/api/v3/brokerage/portfolios"
COINBASE_MAX_PAGES = 100
COINBASE_API_KEY_NAME_ENV = "COINBASE_API_KEY_NAME"
COINBASE_API_KEY_SECRET_ENV = "COINBASE_API_KEY_SECRET"

COINBASE_CSV_HEADER = [
    "source",
    "id",
    "name",
    "currency",
    "currency_name",
    "currency_type",
    "balance",
    "balance_currency",
    "value",
    "value_currency",
    "available",
    "available_currency",
    "account_type",
    "portfolio_name",
    "portfolio_uuid",
    "position_side",
    "avg_entry_price",
    "mark_price",
    "unrealized_pnl",
    "initial_margin",
    "leverage",
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


def _money_value(value: object | None) -> object | None:
    if isinstance(value, dict):
        if "value" in value:
            return value.get("value")
        raw_currency = value.get("rawCurrency")
        if isinstance(raw_currency, dict):
            return raw_currency.get("value")
        user_native_currency = value.get("userNativeCurrency")
        if isinstance(user_native_currency, dict):
            return user_native_currency.get("value")
    return value


def _money_currency(value: object | None) -> object | None:
    if isinstance(value, dict):
        if "currency" in value:
            return value.get("currency")
        raw_currency = value.get("rawCurrency")
        if isinstance(raw_currency, dict):
            return raw_currency.get("currency")
        user_native_currency = value.get("userNativeCurrency")
        if isinstance(user_native_currency, dict):
            return user_native_currency.get("currency")
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
    path: str = COINBASE_ACCOUNTS_PATH,
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
        jwt_token = _build_coinbase_jwt(resolved_key_name, resolved_key_secret, path=path)
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
    path: str,
    headers: dict[str, str],
    params: dict[str, str] | None = None,
) -> dict[str, object]:
    response = await client.get(
        f"{COINBASE_API_BASE_URL}{path}",
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
        payload = await _fetch_coinbase_json(
            client, COINBASE_ACCOUNTS_PATH, headers, params
        )
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


async def _fetch_coinbase_intx_portfolios(
    client: httpx.AsyncClient,
    token: str | None,
    key_name: str | None,
    key_secret: str | None,
) -> list[dict[str, object]]:
    headers = _build_auth_header(token, key_name, key_secret, COINBASE_PORTFOLIOS_PATH)
    payload = await _fetch_coinbase_json(
        client,
        COINBASE_PORTFOLIOS_PATH,
        headers,
        {"portfolio_type": "INTX"},
    )

    portfolios = payload.get("portfolios", [])
    if not isinstance(portfolios, list):
        raise HTTPException(
            status_code=502, detail="Unexpected Coinbase portfolios response format"
        )

    return [
        item
        for item in portfolios
        if isinstance(item, dict) and not bool(item.get("deleted"))
    ]


async def _fetch_coinbase_portfolio_breakdown(
    client: httpx.AsyncClient,
    token: str | None,
    key_name: str | None,
    key_secret: str | None,
    portfolio_uuid: str,
) -> dict[str, object]:
    path = f"{COINBASE_PORTFOLIOS_PATH}/{portfolio_uuid}"
    headers = _build_auth_header(token, key_name, key_secret, path)
    payload = await _fetch_coinbase_json(client, path, headers)

    breakdown = payload.get("breakdown", {})
    if not isinstance(breakdown, dict):
        raise HTTPException(
            status_code=502, detail="Unexpected Coinbase portfolio response format"
        )

    return breakdown


async def _fetch_coinbase_portfolio_breakdowns(
    client: httpx.AsyncClient,
    token: str | None,
    key_name: str | None,
    key_secret: str | None,
) -> list[dict[str, object]]:
    portfolios = await _fetch_coinbase_intx_portfolios(
        client, token, key_name, key_secret
    )

    breakdowns: list[dict[str, object]] = []
    for portfolio in portfolios:
        portfolio_uuid = str(portfolio.get("uuid") or "").strip()
        if not portfolio_uuid:
            continue
        breakdowns.append(
            await _fetch_coinbase_portfolio_breakdown(
                client, token, key_name, key_secret, portfolio_uuid
            )
        )

    return breakdowns


def _portfolio_context(breakdown: dict[str, object]) -> tuple[str, str]:
    portfolio = breakdown.get("portfolio", {})
    if not isinstance(portfolio, dict):
        return "", ""
    return (
        str(portfolio.get("name") or ""),
        str(portfolio.get("uuid") or ""),
    )


def _is_nonzero_value(value: object | None) -> bool:
    return _decimal_or_zero(_money_value(value)) != 0


def _write_portfolio_balance_rows(
    writer: object,
    breakdown: dict[str, object],
    include_zero: bool,
) -> None:
    portfolio_name, portfolio_uuid = _portfolio_context(breakdown)
    balances = breakdown.get("portfolio_balances", {})
    if not isinstance(balances, dict):
        return

    for field in [
        "total_balance",
        "total_futures_balance",
        "total_cash_equivalent_balance",
        "total_crypto_balance",
        "futures_unrealized_pnl",
        "perp_unrealized_pnl",
        "total_equities_balance",
    ]:
        value = balances.get(field)
        if not include_zero and not _is_nonzero_value(value):
            continue

        currency = _money_currency(value)
        writer.writerow(
            [
                "portfolio_balance",
                f"{portfolio_uuid}:{field}",
                field,
                _csv_value(currency),
                "",
                "",
                _normalize_number(_money_value(value)),
                _csv_value(currency),
                _normalize_number(_money_value(value)),
                _csv_value(currency),
                "",
                "",
                "portfolio_balance",
                portfolio_name,
                portfolio_uuid,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "portfolio_balance",
                f"{COINBASE_PORTFOLIOS_PATH}/{portfolio_uuid}",
                "",
                "",
            ]
        )


def _write_spot_position_rows(
    writer: object,
    breakdown: dict[str, object],
    include_zero: bool,
) -> None:
    portfolio_name, portfolio_uuid = _portfolio_context(breakdown)
    positions = breakdown.get("spot_positions", [])
    if not isinstance(positions, list):
        return

    for position in positions:
        if not isinstance(position, dict):
            continue
        if not include_zero and _decimal_or_zero(position.get("total_balance_crypto")) == 0:
            continue

        asset = _csv_value(position.get("asset"))
        writer.writerow(
            [
                "spot_position",
                _csv_value(position.get("account_uuid") or position.get("asset_uuid")),
                asset,
                asset,
                "",
                "cash" if bool(position.get("is_cash")) else "crypto",
                _normalize_number(position.get("total_balance_crypto")),
                asset,
                _normalize_number(position.get("total_balance_fiat")),
                "USD",
                _normalize_number(position.get("available_to_trade_crypto")),
                asset,
                _csv_value(position.get("account_type") or "spot"),
                portfolio_name,
                portfolio_uuid,
                "",
                _normalize_number(_money_value(position.get("average_entry_price"))),
                "",
                _normalize_number(position.get("unrealized_pnl")),
                "",
                "",
                "",
                "",
                "portfolio_spot_position",
                f"{COINBASE_PORTFOLIOS_PATH}/{portfolio_uuid}",
                "",
                "",
            ]
        )


def _perp_base_currency(position: dict[str, object]) -> str:
    symbol = str(position.get("symbol") or position.get("product_id") or "")
    if not symbol:
        return ""
    return symbol.split("-")[0].split(" ")[0]


def _write_perp_position_rows(
    writer: object,
    breakdown: dict[str, object],
    include_zero: bool,
) -> None:
    portfolio_name, portfolio_uuid = _portfolio_context(breakdown)
    positions = breakdown.get("perp_positions", [])
    if not isinstance(positions, list):
        return

    for position in positions:
        if not isinstance(position, dict):
            continue
        if not include_zero and _decimal_or_zero(position.get("net_size")) == 0:
            continue

        currency = _perp_base_currency(position)
        unrealized_pnl = position.get("unrealized_pnl")
        im_notional = position.get("im_notional")
        mark_price = position.get("mark_price")
        position_notional = position.get("position_notional")
        vwap = position.get("vwap")
        writer.writerow(
            [
                "perp_position",
                _csv_value(position.get("product_uuid") or position.get("product_id")),
                _csv_value(position.get("symbol") or position.get("product_id")),
                currency,
                _csv_value(position.get("product_id")),
                "perp",
                _normalize_number(position.get("net_size")),
                currency,
                _normalize_number(_money_value(position_notional)),
                _csv_value(_money_currency(position_notional)),
                "",
                "",
                _csv_value(position.get("margin_type") or "perp"),
                portfolio_name,
                portfolio_uuid,
                _csv_value(position.get("position_side")),
                _normalize_number(_money_value(vwap)),
                _normalize_number(_money_value(mark_price)),
                _normalize_number(_money_value(unrealized_pnl)),
                _normalize_number(
                    position.get("im_contribution") or _money_value(im_notional)
                ),
                _csv_value(position.get("leverage")),
                "",
                "",
                "portfolio_perp_position",
                f"{COINBASE_PORTFOLIOS_PATH}/{portfolio_uuid}",
                "",
                "",
            ]
        )


def _render_coinbase_csv(
    accounts: list[dict[str, object]],
    portfolio_breakdowns: list[dict[str, object]],
    include_zero: bool,
) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(COINBASE_CSV_HEADER)

    for account in accounts:
        if not include_zero and not _has_positive_balance(account):
            continue

        writer.writerow(
            [
                "account",
                _csv_value(account.get("id")),
                _csv_value(account.get("name")),
                _csv_value(_nested_value(account, "currency", "code")),
                _csv_value(_nested_value(account, "currency", "name")),
                _csv_value(_nested_value(account, "currency", "type")),
                _normalize_number(_balance_amount(account)),
                _csv_value(_nested_value(account, "balance", "currency")),
                "",
                "",
                "",
                "",
                _csv_value(account.get("type")),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                _csv_value(account.get("primary")),
                _csv_value(account.get("ready")),
                _csv_value(account.get("resource")),
                _csv_value(account.get("resource_path")),
                _csv_value(account.get("created_at")),
                _csv_value(account.get("updated_at")),
            ]
        )

    for breakdown in portfolio_breakdowns:
        _write_portfolio_balance_rows(writer, breakdown, include_zero)
        _write_spot_position_rows(writer, breakdown, include_zero)
        _write_perp_position_rows(writer, breakdown, include_zero)

    return output.getvalue()


@router.get(
    "/balance",
    summary="Export Coinbase balances for Google Sheets",
    description=(
        "Returns a CSV table with Coinbase App account balances from /v2/accounts "
        "and INTX portfolio balances/positions from /api/v3/brokerage/portfolios. "
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
    include_portfolios: bool = Query(
        True,
        description="Include Coinbase INTX portfolio balances and positions.",
    ),
):
    account_headers = _build_auth_header(token, key_name, key_secret)

    async with httpx.AsyncClient(timeout=20.0) as client:
        accounts = await _fetch_coinbase_accounts(client, account_headers)
        portfolio_breakdowns = (
            await _fetch_coinbase_portfolio_breakdowns(
                client, token, key_name, key_secret
            )
            if include_portfolios
            else []
        )

    return Response(
        content=_render_coinbase_csv(accounts, portfolio_breakdowns, include_zero),
        media_type="text/csv",
    )
