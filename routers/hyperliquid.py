import csv
import io
from decimal import Decimal, InvalidOperation

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response


HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
ALLOWED_BALANCE_FIELDS = {
    "account_value",
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
            {
                "account": address,
                "account_type": "subaccount",
                "master": resolved_address,
                "name": "",
                "account_value": _extract_balance_field(
                    clearinghouse_state, "account_value"
                ),
                "withdrawable": _extract_balance_field(
                    clearinghouse_state, "withdrawable"
                ),
                "spot_balances": _serialize_spot_balances(spot_state),
                "time": str(clearinghouse_state.get("time", "")),
            }
        ]

    main_clearinghouse_state = await _get_clearinghouse_state(client, resolved_address)
    main_spot_state = await _get_spot_state(client, resolved_address)
    subaccounts = await _get_subaccounts(client, resolved_address)

    rows = [
        {
            "account": resolved_address,
            "account_type": "main",
            "master": resolved_address,
            "name": "",
            "account_value": _extract_balance_field(
                main_clearinghouse_state, "account_value"
            ),
            "withdrawable": _extract_balance_field(
                main_clearinghouse_state, "withdrawable"
            ),
            "spot_balances": _serialize_spot_balances(main_spot_state),
            "time": str(main_clearinghouse_state.get("time", "")),
        }
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
            {
                "account": subaccount_user,
                "account_type": "subaccount",
                "master": str(item.get("master", resolved_address)).lower(),
                "name": str(item.get("name", "")),
                "account_value": _extract_balance_field(
                    clearinghouse_state, "account_value"
                ),
                "withdrawable": _extract_balance_field(
                    clearinghouse_state, "withdrawable"
                ),
                "spot_balances": _serialize_spot_balances(spot_state),
                "time": str(clearinghouse_state.get("time", "")),
            }
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
        "or a single plain-text number when `account` is provided."
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
):
    if field not in ALLOWED_BALANCE_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported field '{field}'. Allowed: {', '.join(sorted(ALLOWED_BALANCE_FIELDS))}",
        )

    normalized_address = _normalize_address(address)

    async with httpx.AsyncClient(timeout=20.0) as client:
        rows = await _build_accounts_rows(client, normalized_address)

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
                row["spot_balances"],
                row["time"],
            ]
        )

    return Response(content=output.getvalue(), media_type="text/csv")
