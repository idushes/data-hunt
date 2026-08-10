import csv
import io
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from outbound_queue import queued_async_client


GMX_CACHE_TTL_SECONDS = 60
GMX_NETWORKS = {
    42161: ("Arbitrum", "https://arbitrum.gmxapi.io/v1"),
    43114: ("Avalanche", "https://avalanche.gmxapi.io/v1"),
}
GMX_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "protocol",
    "position_id",
    "contract_key",
    "market_address",
    "market_name",
    "index_token_address",
    "index_token_symbol",
    "pool_name",
    "direction",
    "is_long",
    "collateral_token_address",
    "collateral_token_symbol",
    "collateral_token_name",
    "collateral_amount",
    "collateral_usd",
    "remaining_collateral_amount",
    "remaining_collateral_usd",
    "size_tokens",
    "size_usd",
    "entry_price_usd",
    "mark_price_usd",
    "liquidation_price_usd",
    "leverage",
    "pnl_usd",
    "pnl_percent",
    "pnl_after_fees_usd",
    "pnl_after_fees_percent",
    "pnl_after_all_fees_usd",
    "pnl_after_all_fees_percent",
    "net_value_usd",
    "net_value_after_all_fees_usd",
    "pending_borrowing_fees_usd",
    "pending_funding_fees_usd",
    "pending_claimable_funding_fees_usd",
    "closing_fee_usd",
    "position_fee_amount",
    "funding_fee_amount",
    "claimable_long_token_symbol",
    "claimable_long_token_amount",
    "claimable_short_token_symbol",
    "claimable_short_token_amount",
    "has_low_collateral",
    "increased_at",
    "decreased_at",
    "related_orders_count",
]

router = APIRouter(prefix="/gmx", tags=["gmx"])


def _normalize_wallet(address: str) -> str:
    normalized = address.strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", normalized):
        raise HTTPException(
            status_code=400,
            detail="Address must be a 42-character EVM hex address",
        )
    return normalized.lower()


def _dict(value: object | None) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _decimal(value: object | None) -> Decimal:
    try:
        return Decimal(str(value)) if value is not None else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _format_decimal(value: Decimal) -> str:
    return "0" if value == 0 else format(value.normalize(), "f")


def _fixed(value: object | None, decimals: int) -> str:
    return _format_decimal(_decimal(value) / (Decimal(10) ** decimals))


def _timestamp(value: object | None) -> str:
    seconds = int(_decimal(value))
    if seconds <= 0:
        return ""
    try:
        return datetime.fromtimestamp(seconds, tz=UTC).isoformat()
    except (OverflowError, OSError, ValueError):
        return ""


def _items_by_address(items: object, address_key: str) -> dict[str, dict[str, Any]]:
    if not isinstance(items, list):
        return {}
    result = {}
    for item in items:
        data = _dict(item)
        address = str(data.get(address_key) or "").lower()
        if address:
            result[address] = data
    return result


def _token_amount(value: object | None, token: dict[str, Any]) -> str:
    try:
        decimals = int(token.get("decimals", 0))
    except (TypeError, ValueError):
        decimals = 0
    return _fixed(value, max(0, decimals))


def _parse_rows(
    wallet: str,
    chain_id: int,
    positions: object,
    markets: object,
    tokens: object,
) -> list[dict[str, str]]:
    if not isinstance(positions, list):
        return []
    chain_name = GMX_NETWORKS[chain_id][0]
    market_map = _items_by_address(markets, "marketTokenAddress")
    token_map = _items_by_address(tokens, "address")
    rows = []

    for item in positions:
        position = _dict(item)
        key = str(position.get("key") or "").lower()
        if not key:
            continue
        market_address = str(position.get("marketAddress") or "").lower()
        collateral_address = str(
            position.get("collateralTokenAddress") or ""
        ).lower()
        market = market_map.get(market_address, {})
        collateral = token_map.get(collateral_address, {})
        index_address = str(market.get("indexTokenAddress") or "").lower()
        long_address = str(market.get("longTokenAddress") or "").lower()
        short_address = str(market.get("shortTokenAddress") or "").lower()
        index_token = token_map.get(index_address, {})
        long_token = token_map.get(long_address, {})
        short_token = token_map.get(short_address, {})
        related_orders = position.get("relatedOrders")
        is_long = bool(position.get("isLong"))

        row = {column: "" for column in GMX_CSV_HEADER}
        row.update(
            {
                "wallet": wallet,
                "chain_id": str(chain_id),
                "chain": chain_name,
                "protocol": "GMX V2",
                "position_id": key,
                "contract_key": str(position.get("contractKey") or "").lower(),
                "market_address": market_address,
                "market_name": str(
                    market.get("symbol") or position.get("indexName") or ""
                ),
                "index_token_address": index_address,
                "index_token_symbol": str(
                    index_token.get("symbol") or position.get("indexName") or ""
                ).removesuffix("/USD"),
                "pool_name": str(position.get("poolName") or ""),
                "direction": "long" if is_long else "short",
                "is_long": str(is_long).lower(),
                "collateral_token_address": collateral_address,
                "collateral_token_symbol": str(collateral.get("symbol") or ""),
                "collateral_token_name": str(collateral.get("name") or ""),
                "collateral_amount": _token_amount(
                    position.get("collateralAmount"), collateral
                ),
                "collateral_usd": _fixed(position.get("collateralUsd"), 30),
                "remaining_collateral_amount": _token_amount(
                    position.get("remainingCollateralAmount"), collateral
                ),
                "remaining_collateral_usd": _fixed(
                    position.get("remainingCollateralUsd"), 30
                ),
                "size_tokens": _token_amount(
                    position.get("sizeInTokens"), index_token
                ),
                "size_usd": _fixed(position.get("sizeInUsd"), 30),
                "entry_price_usd": _fixed(position.get("entryPrice"), 30),
                "mark_price_usd": _fixed(position.get("markPrice"), 30),
                "liquidation_price_usd": _fixed(
                    position.get("liquidationPrice"), 30
                ),
                "leverage": _fixed(position.get("leverage"), 4),
                "pnl_usd": _fixed(position.get("pnl"), 30),
                "pnl_percent": _fixed(position.get("pnlPercentage"), 2),
                "pnl_after_fees_usd": _fixed(
                    position.get("pnlAfterFees"), 30
                ),
                "pnl_after_fees_percent": _fixed(
                    position.get("pnlAfterFeesPercentage"), 2
                ),
                "pnl_after_all_fees_usd": _fixed(
                    position.get("pnlAfterAllFees"), 30
                ),
                "pnl_after_all_fees_percent": _fixed(
                    position.get("pnlAfterAllFeesPercentage"), 2
                ),
                "net_value_usd": _fixed(position.get("netValue"), 30),
                "net_value_after_all_fees_usd": _fixed(
                    position.get("netValueAfterAllFees"), 30
                ),
                "pending_borrowing_fees_usd": _fixed(
                    position.get("pendingBorrowingFeesUsd"), 30
                ),
                "pending_funding_fees_usd": _fixed(
                    position.get("pendingFundingFeesUsd"), 30
                ),
                "pending_claimable_funding_fees_usd": _fixed(
                    position.get("pendingClaimableFundingFeesUsd"), 30
                ),
                "closing_fee_usd": _fixed(position.get("closingFeeUsd"), 30),
                "position_fee_amount": _token_amount(
                    position.get("positionFeeAmount"), collateral
                ),
                "funding_fee_amount": _token_amount(
                    position.get("fundingFeeAmount"), collateral
                ),
                "claimable_long_token_symbol": str(long_token.get("symbol") or ""),
                "claimable_long_token_amount": _token_amount(
                    position.get("claimableLongTokenAmount"), long_token
                ),
                "claimable_short_token_symbol": str(
                    short_token.get("symbol") or ""
                ),
                "claimable_short_token_amount": _token_amount(
                    position.get("claimableShortTokenAmount"), short_token
                ),
                "has_low_collateral": str(
                    bool(position.get("hasLowCollateral"))
                ).lower(),
                "increased_at": _timestamp(position.get("increasedAtTime")),
                "decreased_at": _timestamp(position.get("decreasedAtTime")),
                "related_orders_count": str(
                    len(related_orders) if isinstance(related_orders, list) else 0
                ),
            }
        )
        rows.append(row)
    return sorted(rows, key=lambda row: row["position_id"])


async def _get_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, object] | None = None,
) -> object:
    try:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"GMX API returned HTTP {exc.response.status_code}",
        ) from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(status_code=502, detail="GMX API request failed") from exc


async def _fetch_gmx_rows(
    client: httpx.AsyncClient, wallet: str, chain_id: int
) -> list[dict[str, str]]:
    base_url = GMX_NETWORKS[chain_id][1]
    positions = await _get_json(
        client,
        f"{base_url}/positions",
        params={"address": wallet, "includeRelatedOrders": "true"},
    )
    if not isinstance(positions, list):
        raise HTTPException(status_code=502, detail="GMX API returned invalid positions")
    if not positions:
        return []

    markets = await _get_json(client, f"{base_url}/markets")
    tokens = await _get_json(client, f"{base_url}/tokens")
    if not isinstance(markets, list) or not isinstance(tokens, list):
        raise HTTPException(status_code=502, detail="GMX API returned invalid metadata")
    return _parse_rows(wallet, chain_id, positions, markets, tokens)


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=GMX_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export GMX V2 positions",
    description=(
        "Returns open GMX V2 perpetual positions with collateral, size, prices, "
        "leverage, PnL, fees, and claimable funding amounts."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_gmx_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(42161, description="Arbitrum or Avalanche chain ID."),
):
    wallet = _normalize_wallet(address)
    if chain_id not in GMX_NETWORKS:
        supported = ", ".join(str(value) for value in GMX_NETWORKS)
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported GMX chain. Supported chain IDs: {supported}",
        )
    async with queued_async_client(timeout=30.0, trust_env=False) as client:
        rows = await _fetch_gmx_rows(client, wallet, chain_id)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={GMX_CACHE_TTL_SECONDS}"},
    )
