import asyncio
import csv
import io
from decimal import Decimal, InvalidOperation

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response


LIGHTER_API_BASE_URL = "https://mainnet.zklighter.elliot.ai/api/v1"
ALLOWED_BALANCE_FIELDS = {
    "available_balance",
    "collateral",
    "cross_asset_value",
    "total_asset_value",
}

router = APIRouter(prefix="/lighter", tags=["lighter"])


def _normalize_number(value: str | None) -> str:
    if value is None:
        return "0"

    try:
        normalized = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return str(value)

    return format(normalized, "f")


async def _fetch_lighter_json(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, str],
) -> dict:
    response = await client.get(f"{LIGHTER_API_BASE_URL}{path}", params=params)

    if response.status_code >= 400:
        try:
            error_payload = response.json()
            error_message = error_payload.get("message") or response.text
        except Exception:
            error_message = response.text

        raise HTTPException(
            status_code=response.status_code,
            detail=f"Lighter API error: {error_message}",
        )

    try:
        payload = response.json()
    except Exception:
        raise HTTPException(status_code=502, detail="Lighter returned invalid JSON")

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502, detail="Unexpected Lighter response format"
        )

    return payload


async def _fetch_account_by_index(
    client: httpx.AsyncClient, account_index: str
) -> dict[str, object]:
    payload = await _fetch_lighter_json(
        client,
        "/account",
        {"by": "index", "value": account_index},
    )
    accounts = payload.get("accounts", [])
    if not isinstance(accounts, list) or not accounts:
        raise HTTPException(status_code=404, detail="Lighter account not found")

    account = accounts[0]
    if not isinstance(account, dict):
        raise HTTPException(
            status_code=502, detail="Unexpected Lighter response format"
        )

    return account


async def _fetch_accounts_for_address(
    client: httpx.AsyncClient, address: str
) -> list[dict[str, object]]:
    payload = await _fetch_lighter_json(
        client,
        "/accountsByL1Address",
        {"l1_address": address},
    )
    sub_accounts = payload.get("sub_accounts", [])
    if not isinstance(sub_accounts, list):
        raise HTTPException(
            status_code=502, detail="Unexpected Lighter response format"
        )

    indexes = []
    for item in sub_accounts:
        if not isinstance(item, dict):
            continue
        index = item.get("index") or item.get("account_index")
        if index is not None:
            indexes.append(str(index))

    if not indexes:
        return []

    accounts = await asyncio.gather(
        *[_fetch_account_by_index(client, account_index) for account_index in indexes]
    )
    return accounts


@router.get(
    "/balance",
    summary="Export Lighter balance for Google Sheets",
    description=(
        "Returns a CSV table for all Lighter accounts linked to an L1 wallet address, "
        "or a single plain-text number when `account` is provided."
    ),
    responses={
        200: {
            "content": {
                "text/csv": {},
                "text/plain": {},
            }
        }
    },
)
async def get_lighter_balance(
    address: str = Query(
        ..., description="L1 wallet address linked to Lighter accounts."
    ),
    account: str | None = Query(
        None,
        description="Specific Lighter account_index. If set, response is a single number.",
    ),
    field: str = Query(
        "total_asset_value",
        description="Balance field to return. Default is total_asset_value.",
    ),
):
    normalized_address = address.strip()
    if not normalized_address:
        raise HTTPException(status_code=400, detail="Address is required")

    if field not in ALLOWED_BALANCE_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported field '{field}'. Allowed: {', '.join(sorted(ALLOWED_BALANCE_FIELDS))}",
        )

    async with httpx.AsyncClient(timeout=20.0) as client:
        accounts = await _fetch_accounts_for_address(client, normalized_address)

    if account:
        requested_account = account.strip()
        matched_item = next(
            (
                item
                for item in accounts
                if str(item.get("account_index") or item.get("index") or "")
                == requested_account
            ),
            None,
        )

        if matched_item is None:
            raise HTTPException(status_code=404, detail="Lighter account not found")

        value = _normalize_number(matched_item.get(field))
        return Response(content=value, media_type="text/plain")

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "account_index",
            "account_type",
            "l1_address",
            "total_asset_value",
            "cross_asset_value",
            "collateral",
            "available_balance",
            "status",
            "name",
        ]
    )

    for item in accounts:
        account_type = str(item.get("account_type", ""))
        writer.writerow(
            [
                item.get("account_index") or item.get("index", ""),
                "main" if account_type == "0" else "subaccount",
                item.get("l1_address", ""),
                _normalize_number(item.get("total_asset_value")),
                _normalize_number(item.get("cross_asset_value")),
                _normalize_number(item.get("collateral")),
                _normalize_number(item.get("available_balance")),
                item.get("status", ""),
                item.get("name", ""),
            ]
        )

    return Response(content=output.getvalue(), media_type="text/csv")
