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


def _parse_readonly_token(token: str) -> str:
    normalized_token = token.strip()
    if not normalized_token:
        raise HTTPException(status_code=400, detail="Token is required")

    parts = normalized_token.split(":")
    if len(parts) < 5 or parts[0] != "ro" or not parts[1].isdigit():
        raise HTTPException(status_code=400, detail="Invalid Lighter readonly token")

    return parts[1]


def _parse_accounts_csv(accounts: str) -> list[str]:
    parsed_accounts = []
    seen_accounts = set()

    for raw_item in accounts.split(","):
        account_index = raw_item.strip()
        if not account_index:
            continue
        if not account_index.isdigit():
            raise HTTPException(
                status_code=400,
                detail="Lighter accounts must be numeric account_index values",
            )
        if account_index not in seen_accounts:
            seen_accounts.add(account_index)
            parsed_accounts.append(account_index)

    if not parsed_accounts:
        raise HTTPException(status_code=400, detail="At least one account is required")

    return parsed_accounts


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

    accounts = []
    for account_index in indexes:
        accounts.append(await _fetch_account_by_index(client, account_index))

    return accounts


async def _resolve_accounts(
    client: httpx.AsyncClient,
    token: str | None,
    account: str | None,
    accounts: str | None,
    address: str | None,
) -> tuple[list[dict[str, object]], bool]:
    provided_args = [
        token is not None,
        account is not None,
        accounts is not None,
        address is not None,
    ]
    if sum(provided_args) != 1:
        raise HTTPException(
            status_code=400,
            detail="Provide exactly one of token, account, accounts, or address",
        )

    if token is not None:
        resolved_account = _parse_readonly_token(token)
        return [await _fetch_account_by_index(client, resolved_account)], True

    if account is not None:
        normalized_account = account.strip()
        if not normalized_account or not normalized_account.isdigit():
            raise HTTPException(
                status_code=400,
                detail="Lighter account must be a numeric account_index",
            )
        return [await _fetch_account_by_index(client, normalized_account)], True

    if accounts is not None:
        parsed_accounts = _parse_accounts_csv(accounts)
        return [
            await _fetch_account_by_index(client, item) for item in parsed_accounts
        ], False

    normalized_address = address.strip() if address is not None else ""
    if not normalized_address:
        raise HTTPException(status_code=400, detail="Address is required")

    try:
        return await _fetch_accounts_for_address(client, normalized_address), False
    except HTTPException as exc:
        if exc.status_code == 400 and "account not found" in str(exc.detail).lower():
            raise HTTPException(
                status_code=404,
                detail=(
                    "Lighter could not find accounts for this address. "
                    "Use token or account_index if you already know the account."
                ),
            )
        raise


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
    token: str | None = Query(
        None,
        description="Lighter readonly token. Used only to extract account_index.",
    ),
    account: str | None = Query(
        None,
        description="Specific Lighter account_index. If set, response is a single number.",
    ),
    accounts: str | None = Query(
        None,
        description="Comma-separated Lighter account_index values. Returns a CSV table.",
    ),
    address: str | None = Query(
        None,
        description="L1 wallet address linked to Lighter accounts. Best-effort fallback.",
    ),
    field: str = Query(
        "total_asset_value",
        description="Balance field to return. Default is total_asset_value.",
    ),
):
    if field not in ALLOWED_BALANCE_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported field '{field}'. Allowed: {', '.join(sorted(ALLOWED_BALANCE_FIELDS))}",
        )

    async with httpx.AsyncClient(timeout=20.0) as client:
        resolved_accounts, single_value_response = await _resolve_accounts(
            client,
            token,
            account,
            accounts,
            address,
        )

    if single_value_response:
        value = _normalize_number(resolved_accounts[0].get(field))
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

    for item in resolved_accounts:
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
