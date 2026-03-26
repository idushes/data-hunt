import csv
import io
from decimal import Decimal, InvalidOperation

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response


PARADEX_API_BASE_URL = "https://api.prod.paradex.trade/v1"
ALLOWED_BALANCE_FIELDS = {
    "account_value",
    "total_collateral",
    "free_collateral",
    "initial_margin_requirement",
    "maintenance_margin_requirement",
    "margin_cushion",
}

router = APIRouter(prefix="/paradex", tags=["paradex"])


def _normalize_number(value: str | None) -> str:
    if value is None:
        return "0"

    try:
        normalized = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return str(value)

    return format(normalized, "f")


def _build_auth_header(token: str) -> dict[str, str]:
    token = token.strip()
    if not token:
        raise HTTPException(status_code=400, detail="Token is required")

    if not token.lower().startswith("bearer "):
        token = f"Bearer {token}"

    return {"Authorization": token, "Accept": "application/json"}


async def _fetch_paradex_json(
    client: httpx.AsyncClient,
    path: str,
    token: str,
    params: dict[str, str] | None = None,
) -> dict:
    response = await client.get(
        f"{PARADEX_API_BASE_URL}{path}",
        headers=_build_auth_header(token),
        params=params,
    )

    if response.status_code == 401:
        raise HTTPException(status_code=401, detail="Invalid Paradex token")

    if response.status_code >= 400:
        try:
            error_payload = response.json()
            error_message = error_payload.get("message") or response.text
        except Exception:
            error_message = response.text

        raise HTTPException(
            status_code=response.status_code,
            detail=f"Paradex API error: {error_message}",
        )

    try:
        return response.json()
    except Exception:
        raise HTTPException(status_code=502, detail="Paradex returned invalid JSON")


@router.get(
    "/balance",
    summary="Export Paradex balance for Google Sheets",
    description=(
        "Returns a CSV table for all available Paradex accounts, or a single plain-text "
        "number when `account` is provided. Token is accepted via query parameter for "
        "Google Sheets compatibility."
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
async def get_paradex_balance(
    token: str = Query(
        ..., description="Paradex token. Can be passed with or without Bearer prefix."
    ),
    account: str | None = Query(
        None,
        description="Specific Paradex subaccount address. If set, response is a single number.",
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

    async with httpx.AsyncClient(timeout=20.0) as client:
        payload = await _fetch_paradex_json(client, "/account/summary", token)

    if isinstance(payload, list):
        results = payload
    elif isinstance(payload, dict):
        results = payload.get("results", [])
    else:
        raise HTTPException(
            status_code=502, detail="Unexpected Paradex response format"
        )

    if not isinstance(results, list):
        raise HTTPException(
            status_code=502, detail="Unexpected Paradex response format"
        )

    if account:
        normalized_account = account.lower()
        matched_item = next(
            (
                item
                for item in results
                if isinstance(item, dict)
                and str(item.get("account", "")).lower() == normalized_account
            ),
            None,
        )

        if matched_item is None:
            raise HTTPException(status_code=404, detail="Paradex account not found")

        value = _normalize_number(matched_item.get(field))
        return Response(content=value, media_type="text/plain")

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "account",
            "account_value",
            "total_collateral",
            "free_collateral",
            "status",
            "settlement_asset",
            "updated_at",
        ]
    )

    for item in results:
        writer.writerow(
            [
                item.get("account", ""),
                _normalize_number(item.get("account_value")),
                _normalize_number(item.get("total_collateral")),
                _normalize_number(item.get("free_collateral")),
                item.get("status", ""),
                item.get("settlement_asset", ""),
                item.get("updated_at", ""),
            ]
        )

    return Response(content=output.getvalue(), media_type="text/csv")
