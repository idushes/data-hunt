import asyncio
import csv
import io
import os
import re
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from outbound_queue import queued_async_client


POLYMARKET_API_URL = "https://data-api.polymarket.com"
POLYMARKET_POLYGON_RPC_URL = "https://polygon-bor-rpc.publicnode.com"
POLYMARKET_PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYMARKET_PUSD_DECIMALS = 6
BALANCE_OF_SELECTOR = "70a08231"
POLYMARKET_CACHE_TTL_SECONDS = 60
POLYMARKET_PAGE_SIZE = 500
POLYMARKET_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "protocol",
    "position_id",
    "row_type",
    "token_symbol",
    "token_address",
    "balance",
    "balance_usd",
    "asset_id",
    "condition_id",
    "title",
    "market_slug",
    "event_slug",
    "market_url",
    "icon_url",
    "outcome",
    "outcome_index",
    "opposite_outcome",
    "opposite_asset_id",
    "size",
    "average_price",
    "current_price",
    "initial_value_usd",
    "current_value_usd",
    "cash_pnl_usd",
    "percent_pnl",
    "realized_pnl_usd",
    "percent_realized_pnl",
    "total_bought",
    "redeemable",
    "mergeable",
    "end_date",
    "negative_risk",
    "portfolio_value_usd",
    "total_account_value_usd",
]

router = APIRouter(prefix="/polymarket", tags=["polymarket"])


def _normalize_wallet(address: str) -> str:
    normalized = address.strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", normalized):
        raise HTTPException(
            status_code=400,
            detail="Address must be a 42-character EVM hex address",
        )
    return normalized.lower()


def _decimal(value: object | None) -> Decimal:
    try:
        return Decimal(str(value)) if value is not None else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _format_decimal(value: object | Decimal | None) -> str:
    normalized = _decimal(value)
    return "0" if normalized == 0 else format(normalized.normalize(), "f")


def _boolean(value: object | None) -> str:
    return str(value is True).lower()


async def _get_json(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, object],
) -> object:
    try:
        response = await client.get(f"{POLYMARKET_API_URL}{path}", params=params)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Polymarket API returned HTTP {exc.response.status_code}",
        ) from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502,
            detail="Polymarket API request failed",
        ) from exc


async def _fetch_positions(
    client: httpx.AsyncClient,
    wallet: str,
    size_threshold: float,
) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    offset = 0
    while offset <= 10_000:
        payload = await _get_json(
            client,
            "/positions",
            {
                "user": wallet,
                "sizeThreshold": size_threshold,
                "limit": POLYMARKET_PAGE_SIZE,
                "offset": offset,
                "sortBy": "CURRENT",
                "sortDirection": "DESC",
            },
        )
        if not isinstance(payload, list):
            raise HTTPException(
                status_code=502,
                detail="Polymarket API returned invalid positions",
            )
        page = [item for item in payload if isinstance(item, dict)]
        positions.extend(page)
        if len(payload) < POLYMARKET_PAGE_SIZE:
            break
        offset += POLYMARKET_PAGE_SIZE
    return positions


async def _fetch_pusd_balance(
    client: httpx.AsyncClient,
    wallet: str,
) -> Decimal:
    encoded_wallet = wallet[2:].lower().rjust(64, "0")
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            {
                "to": POLYMARKET_PUSD_ADDRESS,
                "data": f"0x{BALANCE_OF_SELECTOR}{encoded_wallet}",
            },
            "latest",
        ],
    }
    rpc_url = os.getenv("POLYMARKET_POLYGON_RPC_URL") or POLYMARKET_POLYGON_RPC_URL
    try:
        response = await client.post(rpc_url, json=payload)
        response.raise_for_status()
        result = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502,
            detail="Polygon RPC request for Polymarket pUSD failed",
        ) from exc

    raw_balance = result.get("result") if isinstance(result, dict) else None
    try:
        amount = int(raw_balance, 16) if isinstance(raw_balance, str) else None
    except ValueError:
        amount = None
    if amount is None:
        raise HTTPException(
            status_code=502,
            detail="Polygon RPC returned an invalid Polymarket pUSD balance",
        )
    return Decimal(amount) / (Decimal(10) ** POLYMARKET_PUSD_DECIMALS)


def _portfolio_value(payload: object, wallet: str) -> Decimal:
    if not isinstance(payload, list):
        raise HTTPException(
            status_code=502,
            detail="Polymarket API returned invalid portfolio value",
        )
    for item in payload:
        if not isinstance(item, dict):
            continue
        item_wallet = str(item.get("user") or "").lower()
        if not item_wallet or item_wallet == wallet:
            return _decimal(item.get("value"))
    return Decimal(0)


def _position_row(
    wallet: str,
    position: dict[str, Any],
    portfolio_value: Decimal,
    total_account_value: Decimal,
) -> dict[str, str]:
    asset = str(position.get("asset") or "")
    event_slug = str(position.get("eventSlug") or "")
    row = {column: "" for column in POLYMARKET_CSV_HEADER}
    row.update(
        {
            "wallet": wallet,
            "chain_id": "137",
            "chain": "Polygon",
            "protocol": "Polymarket",
            "position_id": asset,
            "row_type": "market_position",
            "asset_id": asset,
            "condition_id": str(position.get("conditionId") or ""),
            "title": str(position.get("title") or ""),
            "market_slug": str(position.get("slug") or ""),
            "event_slug": event_slug,
            "market_url": (
                f"https://polymarket.com/event/{event_slug}" if event_slug else ""
            ),
            "icon_url": str(position.get("icon") or ""),
            "outcome": str(position.get("outcome") or ""),
            "outcome_index": str(position.get("outcomeIndex") or "0"),
            "opposite_outcome": str(position.get("oppositeOutcome") or ""),
            "opposite_asset_id": str(position.get("oppositeAsset") or ""),
            "size": _format_decimal(position.get("size")),
            "average_price": _format_decimal(position.get("avgPrice")),
            "current_price": _format_decimal(position.get("curPrice")),
            "initial_value_usd": _format_decimal(position.get("initialValue")),
            "current_value_usd": _format_decimal(position.get("currentValue")),
            "cash_pnl_usd": _format_decimal(position.get("cashPnl")),
            "percent_pnl": _format_decimal(position.get("percentPnl")),
            "realized_pnl_usd": _format_decimal(position.get("realizedPnl")),
            "percent_realized_pnl": _format_decimal(position.get("percentRealizedPnl")),
            "total_bought": _format_decimal(position.get("totalBought")),
            "redeemable": _boolean(position.get("redeemable")),
            "mergeable": _boolean(position.get("mergeable")),
            "end_date": str(position.get("endDate") or ""),
            "negative_risk": _boolean(position.get("negativeRisk")),
            "portfolio_value_usd": _format_decimal(portfolio_value),
            "total_account_value_usd": _format_decimal(total_account_value),
        }
    )
    return row


def _parse_rows(
    wallet: str,
    positions: list[dict[str, Any]],
    portfolio_value: Decimal,
    pusd_balance: Decimal = Decimal(0),
) -> list[dict[str, str]]:
    total_account_value = portfolio_value + pusd_balance
    initial_value = sum(
        (_decimal(position.get("initialValue")) for position in positions),
        Decimal(0),
    )
    cash_pnl = sum(
        (_decimal(position.get("cashPnl")) for position in positions),
        Decimal(0),
    )
    realized_pnl = sum(
        (_decimal(position.get("realizedPnl")) for position in positions),
        Decimal(0),
    )
    percent_pnl = (
        cash_pnl / initial_value * Decimal(100) if initial_value != 0 else Decimal(0)
    )
    summary = {column: "" for column in POLYMARKET_CSV_HEADER}
    summary.update(
        {
            "wallet": wallet,
            "chain_id": "137",
            "chain": "Polygon",
            "protocol": "Polymarket",
            "position_id": f"{wallet}:portfolio",
            "row_type": "portfolio_summary",
            "initial_value_usd": _format_decimal(initial_value),
            "current_value_usd": _format_decimal(portfolio_value),
            "cash_pnl_usd": _format_decimal(cash_pnl),
            "percent_pnl": _format_decimal(percent_pnl),
            "realized_pnl_usd": _format_decimal(realized_pnl),
            "portfolio_value_usd": _format_decimal(portfolio_value),
            "total_account_value_usd": _format_decimal(total_account_value),
        }
    )
    pusd = {column: "" for column in POLYMARKET_CSV_HEADER}
    pusd.update(
        {
            "wallet": wallet,
            "chain_id": "137",
            "chain": "Polygon",
            "protocol": "Polymarket",
            "position_id": f"{wallet}:pusd",
            "row_type": "collateral_balance",
            "token_symbol": "pUSD",
            "token_address": POLYMARKET_PUSD_ADDRESS,
            "balance": _format_decimal(pusd_balance),
            "balance_usd": _format_decimal(pusd_balance),
            "title": "Polymarket USD",
            "current_price": "1",
            "current_value_usd": _format_decimal(pusd_balance),
            "portfolio_value_usd": _format_decimal(portfolio_value),
            "total_account_value_usd": _format_decimal(total_account_value),
        }
    )
    rows = [
        _position_row(wallet, position, portfolio_value, total_account_value)
        for position in positions
        if position.get("asset") is not None
    ]
    rows.sort(key=lambda row: row["position_id"])
    return [summary, pusd, *rows]


async def _fetch_polymarket_rows(
    client: httpx.AsyncClient,
    wallet: str,
    size_threshold: float,
) -> list[dict[str, str]]:
    positions, value_payload, pusd_balance = await asyncio.gather(
        _fetch_positions(client, wallet, size_threshold),
        _get_json(client, "/value", {"user": wallet}),
        _fetch_pusd_balance(client, wallet),
    )
    return _parse_rows(
        wallet,
        positions,
        _portfolio_value(value_payload, wallet),
        pusd_balance,
    )


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=POLYMARKET_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export Polymarket positions",
    description=(
        "Returns a Polymarket portfolio summary, on-chain pUSD collateral balance, "
        "and current market positions with prices, value, PnL, outcomes, and "
        "redeemable status. Use the Polymarket funder address: an existing "
        "profile proxy/Safe or the deposit wallet that holds pUSD."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_polymarket_positions_csv(
    address: str = Query(..., description="Polymarket funder wallet address."),
    size_threshold: float = Query(
        1,
        ge=0,
        description="Minimum outcome-token position size. Set to 0 to include dust.",
    ),
):
    wallet = _normalize_wallet(address)
    async with queued_async_client(timeout=30.0, trust_env=False) as client:
        rows = await _fetch_polymarket_rows(client, wallet, size_threshold)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={POLYMARKET_CACHE_TTL_SECONDS}"},
    )
