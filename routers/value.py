import csv
import io
from dataclasses import dataclass

import httpx
from fastapi import APIRouter, HTTPException, Query, Request, Response


@dataclass(frozen=True)
class ValueSource:
    path: str
    key_column: str


VALUE_SOURCES = {
    "debt": ValueSource("/debt", "id"),
    "stability": ValueSource("/stability", "id"),
    "pool": ValueSource("/pool", "id"),
    "wallet": ValueSource("/wallet", "id"),
    "paradex": ValueSource("/paradex/balance", "account"),
    "lighter": ValueSource("/lighter/balance", "account_index"),
    "hyperliquid": ValueSource("/hyperliquid/balance", "account"),
    "coinbase": ValueSource("/coinbase/balance", "id"),
    "gmtrade-assets": ValueSource("/solana/gmtrade.csv", "mint"),
    "gmtrade-perps": ValueSource(
        "/solana/gmtrade-perps.csv", "position_address"
    ),
    "kamino-vaults": ValueSource("/solana/kamino.csv", "vault_address"),
    "kamino-positions": ValueSource(
        "/solana/kamino-positions.csv", "vault_address"
    ),
    "fluid": ValueSource("/fluid/positions.csv", "position_id"),
    "aave": ValueSource("/aave/positions.csv", "position_id"),
    "uniswap": ValueSource("/uniswap/positions.csv", "position_id"),
    "stablecoins": ValueSource("/stablecoins/balances.csv", "balance_id"),
    "stakedao": ValueSource("/stakedao/positions.csv", "position_id"),
}
VALUE_CONTROL_PARAMS = {"source", "key", "column"}

router = APIRouter(tags=["values"])


def _source_error(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text or "Source request failed"

    if isinstance(payload, dict) and isinstance(payload.get("detail"), str):
        return payload["detail"]
    return response.text or "Source request failed"


def _render_single_cell(value: str) -> str:
    output = io.StringIO()
    csv.writer(output).writerow([value])
    return output.getvalue().rstrip("\r\n")


@router.get(
    "/value",
    summary="Get one stable value for Google Sheets",
    description=(
        "Finds a row in a supported CSV source by its stable key and returns one "
        "selected column as a single-cell CSV. Row order and table size do not "
        "affect the result. All additional query parameters are forwarded to the "
        "selected source."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_stable_value(
    request: Request,
    source: str = Query(..., description="Registered CSV source alias."),
    key: str = Query(..., description="Exact stable row identifier."),
    column: str = Query(..., description="CSV column whose value should be returned."),
):
    source_config = VALUE_SOURCES.get(source)
    if source_config is None:
        supported = ", ".join(sorted(VALUE_SOURCES))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported value source. Supported: {supported}",
        )

    forwarded_query = [
        (name, value)
        for name, value in request.query_params.multi_items()
        if name not in VALUE_CONTROL_PARAMS
    ]
    forwarded_headers = {}
    authorization = request.headers.get("authorization")
    if authorization:
        forwarded_headers["authorization"] = authorization

    transport = httpx.ASGITransport(app=request.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://data-hunt.internal",
        timeout=30.0,
    ) as client:
        source_response = await client.get(
            source_config.path,
            params=forwarded_query,
            headers=forwarded_headers,
        )

    if source_response.status_code >= 400:
        raise HTTPException(
            status_code=source_response.status_code,
            detail=_source_error(source_response),
        )

    content_type = source_response.headers.get("content-type", "").lower()
    if not content_type.startswith("text/csv"):
        raise HTTPException(
            status_code=400,
            detail=(
                "The selected source returned a single value instead of a table. "
                "Use that source URL directly."
            ),
        )

    reader = csv.DictReader(io.StringIO(source_response.text))
    fieldnames = reader.fieldnames or []
    if source_config.key_column not in fieldnames:
        raise HTTPException(
            status_code=502,
            detail=f"Source CSV is missing key column '{source_config.key_column}'",
        )
    if column not in fieldnames:
        raise HTTPException(
            status_code=400,
            detail=f"Column '{column}' was not found in the source CSV",
        )

    matches = [
        row
        for row in reader
        if (row.get(source_config.key_column) or "") == key
    ]
    if not matches:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Row with {source_config.key_column}='{key}' was not found"
            ),
        )
    if len(matches) > 1:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Stable key {source_config.key_column}='{key}' is not unique"
            ),
        )

    return Response(
        content=_render_single_cell(matches[0].get(column) or ""),
        media_type="text/csv",
        headers={
            "X-Value-Source": source,
            "X-Value-Key-Column": source_config.key_column,
        },
    )
