import csv
import io
from decimal import Decimal, InvalidOperation

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from outbound_queue import queued_async_client


HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
ALLOWED_BALANCE_FIELDS = {
    "account_value",
    "spot_usdc",
    "total_equity",
    "withdrawable",
}

router = APIRouter(prefix="/hyperliquid", tags=["hyperliquid"])


def _normalize_number(value: str | None) -> str:
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


def _normalize_address(address: str, field_name: str = "Address") -> str:
    normalized = address.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    if not normalized.startswith("0x") or len(normalized) != 42:
        raise HTTPException(
            status_code=400, detail=f"{field_name} must be a 42-character hex address"
        )
    return normalized.lower()


def _extract_balance_field(account_data: dict[str, object], field: str) -> str:
    if field == "account_value":
        margin_summary = account_data.get("marginSummary", {})
        if isinstance(margin_summary, dict):
            return _normalize_number(margin_summary.get("accountValue"))
        return "0"

    if field == "withdrawable":
        return _normalize_number(account_data.get("withdrawable"))

    raise HTTPException(status_code=400, detail=f"Unsupported field '{field}'")


def _extract_spot_coin_totals(
    spot_state: dict[str, object] | None, coin: str
) -> tuple[Decimal, Decimal]:
    if not isinstance(spot_state, dict):
        return Decimal("0"), Decimal("0")

    balances = spot_state.get("balances", [])
    if not isinstance(balances, list):
        return Decimal("0"), Decimal("0")

    total = Decimal("0")
    hold = Decimal("0")
    normalized_coin = coin.upper()

    for item in balances:
        if not isinstance(item, dict):
            continue
        if str(item.get("coin", "")).upper() != normalized_coin:
            continue

        total += _decimal_or_zero(item.get("total"))
        hold += _decimal_or_zero(item.get("hold"))

    return total, hold


def _build_account_row(
    account: str,
    account_type: str,
    master: str,
    name: str,
    clearinghouse_state: dict[str, object],
    spot_state: dict[str, object] | None,
) -> dict[str, str]:
    account_value = _extract_balance_field(clearinghouse_state, "account_value")
    spot_usdc, spot_usdc_hold = _extract_spot_coin_totals(spot_state, "USDC")
    total_equity = spot_usdc - spot_usdc_hold + _decimal_or_zero(account_value)

    return {
        "account": account,
        "account_type": account_type,
        "master": master,
        "name": name,
        "account_value": account_value,
        "withdrawable": _extract_balance_field(clearinghouse_state, "withdrawable"),
        "spot_usdc": format(spot_usdc, "f"),
        "total_equity": format(total_equity, "f"),
        "spot_balances": _serialize_spot_balances(spot_state),
        "time": str(clearinghouse_state.get("time", "")),
    }


def _sum_balance_field(rows: list[dict[str, str]], field: str) -> str:
    total = sum((_decimal_or_zero(row.get(field)) for row in rows), Decimal("0"))
    return format(total, "f")


def _serialize_spot_balances(spot_state: dict[str, object] | None) -> str:
    if not isinstance(spot_state, dict):
        return ""

    balances = spot_state.get("balances", [])
    if not isinstance(balances, list):
        return ""

    serialized = []
    for item in balances:
        if not isinstance(item, dict):
            continue
        coin = item.get("coin", "")
        total = _normalize_number(item.get("total"))
        hold = _normalize_number(item.get("hold"))
        serialized.append(f"{coin}:{total} (hold:{hold})")

    return "; ".join(serialized)


async def _post_info(
    client: httpx.AsyncClient, payload: dict[str, object]
) -> dict | list:
    response = await client.post(HYPERLIQUID_INFO_URL, json=payload)

    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Hyperliquid API error: {response.text}",
        )

    try:
        return response.json()
    except Exception:
        raise HTTPException(status_code=502, detail="Hyperliquid returned invalid JSON")


async def _get_user_role(client: httpx.AsyncClient, address: str) -> dict[str, object]:
    payload = await _post_info(client, {"type": "userRole", "user": address})
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502, detail="Unexpected Hyperliquid response format"
        )
    return payload


async def _get_clearinghouse_state(
    client: httpx.AsyncClient, address: str
) -> dict[str, object]:
    payload = await _post_info(client, {"type": "clearinghouseState", "user": address})
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502, detail="Unexpected Hyperliquid response format"
        )
    return payload


async def _get_spot_state(client: httpx.AsyncClient, address: str) -> dict[str, object]:
    payload = await _post_info(
        client, {"type": "spotClearinghouseState", "user": address}
    )
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502, detail="Unexpected Hyperliquid response format"
        )
    return payload


async def _get_subaccounts(
    client: httpx.AsyncClient, address: str
) -> list[dict[str, object]]:
    payload = await _post_info(client, {"type": "subAccounts", "user": address})
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise HTTPException(
            status_code=502, detail="Unexpected Hyperliquid response format"
        )

    return [item for item in payload if isinstance(item, dict)]


async def _resolve_master_address(
    client: httpx.AsyncClient, address: str
) -> tuple[str, str]:
    role_payload = await _get_user_role(client, address)
    role = str(role_payload.get("role", ""))

    if role == "agent":
        data = role_payload.get("data", {})
        if isinstance(data, dict) and isinstance(data.get("user"), str):
            return _normalize_address(data["user"], "Resolved address"), "agent"
    if role == "subAccount":
        data = role_payload.get("data", {})
        if isinstance(data, dict) and isinstance(data.get("master"), str):
            return _normalize_address(data["master"], "Resolved address"), "subAccount"
    if role in {"user", "vault", "subAccount", "agent"}:
        return address, role
    if role == "missing":
        raise HTTPException(status_code=404, detail="Hyperliquid account not found")

    return address, role


async def _build_accounts_rows(
    client: httpx.AsyncClient, address: str
) -> list[dict[str, str]]:
    resolved_address, role = await _resolve_master_address(client, address)

    if role == "subAccount":
        clearinghouse_state = await _get_clearinghouse_state(client, address)
        spot_state = await _get_spot_state(client, address)
        return [
            _build_account_row(
                address,
                "subaccount",
                resolved_address,
                "",
                clearinghouse_state,
                spot_state,
            )
        ]

    main_clearinghouse_state = await _get_clearinghouse_state(client, resolved_address)
    main_spot_state = await _get_spot_state(client, resolved_address)
    subaccounts = await _get_subaccounts(client, resolved_address)

    rows = [
        _build_account_row(
            resolved_address,
            "main",
            resolved_address,
            "",
            main_clearinghouse_state,
            main_spot_state,
        )
    ]

    for item in subaccounts:
        clearinghouse_state = item.get("clearinghouseState", {})
        spot_state = item.get("spotState", {})
        if not isinstance(clearinghouse_state, dict):
            clearinghouse_state = {}
        if not isinstance(spot_state, dict):
            spot_state = {}

        subaccount_user = str(item.get("subAccountUser", "")).lower()
        if not subaccount_user:
            continue

        rows.append(
            _build_account_row(
                subaccount_user,
                "subaccount",
                str(item.get("master", resolved_address)).lower(),
                str(item.get("name", "")),
                clearinghouse_state,
                spot_state,
            )
        )

    deduped_rows = []
    seen_accounts = set()
    for row in rows:
        account = row["account"]
        if account in seen_accounts:
            continue
        seen_accounts.add(account)
        deduped_rows.append(row)

    return deduped_rows


@router.get(
    "/balance",
    summary="Export Hyperliquid balance for Google Sheets",
    description=(
        "Returns a CSV table for a Hyperliquid main account and its subaccounts, "
        "or a single plain-text number when `account` or `aggregate` is provided."
    ),
    responses={200: {"content": {"text/csv": {}, "text/plain": {}}}},
)
async def get_hyperliquid_balance(
    address: str = Query(
        ..., description="Hyperliquid master, subaccount, or agent address."
    ),
    account: str | None = Query(
        None,
        description="Specific Hyperliquid account address. If set, response is a single number.",
    ),
    field: str = Query(
        "account_value",
        description="Balance field to return. Default is account_value.",
    ),
    aggregate: bool = Query(
        False,
        description=(
            "Return one number by summing the selected field across the main account "
            "and subaccounts."
        ),
    ),
):
    if field not in ALLOWED_BALANCE_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported field '{field}'. Allowed: {', '.join(sorted(ALLOWED_BALANCE_FIELDS))}",
        )

    if account and aggregate:
        raise HTTPException(
            status_code=400, detail="Use either account or aggregate, not both"
        )

    normalized_address = _normalize_address(address)

    async with queued_async_client(timeout=20.0) as client:
        rows = await _build_accounts_rows(client, normalized_address)

        if aggregate:
            return Response(
                content=_sum_balance_field(rows, field), media_type="text/plain"
            )

        if account:
            normalized_account = _normalize_address(account, "Account")
            matched_row = next(
                (row for row in rows if row["account"] == normalized_account), None
            )
            if matched_row is None:
                raise HTTPException(
                    status_code=404, detail="Hyperliquid account not found"
                )
            return Response(content=matched_row[field], media_type="text/plain")

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "account",
            "account_type",
            "master",
            "name",
            "account_value",
            "withdrawable",
            "spot_usdc",
            "total_equity",
            "spot_balances",
            "time",
        ]
    )

    for row in rows:
        writer.writerow(
            [
                row["account"],
                row["account_type"],
                row["master"],
                row["name"],
                row["account_value"],
                row["withdrawable"],
                row["spot_usdc"],
                row["total_equity"],
                row["spot_balances"],
                row["time"],
            ]
        )

    return Response(content=output.getvalue(), media_type="text/csv")
