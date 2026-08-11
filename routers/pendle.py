import csv
import io
import re
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from outbound_queue import queued_async_client


PENDLE_API_URL = "https://api-v2.pendle.finance/core"
PENDLE_CACHE_TTL_SECONDS = 60
PENDLE_CHAIN_NAMES = {
    1: "Ethereum",
    10: "Optimism",
    56: "BNB Chain",
    143: "Monad",
    146: "Sonic",
    196: "X Layer",
    999: "HyperEVM",
    5000: "Mantle",
    8453: "Base",
    9745: "Plasma",
    42161: "Arbitrum",
    57073: "Ink",
    80094: "Berachain",
    747474: "Katana",
}
PENDLE_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "protocol",
    "position_id",
    "row_type",
    "status",
    "market_id",
    "asset_type",
    "token_id",
    "balance_raw",
    "active_balance_raw",
    "value_usd",
    "claimable_token_id",
    "claimable_amount_raw",
    "updated_at",
    "portfolio_value_usd",
    "open_market_count",
    "closed_market_count",
    "sy_position_count",
    "claimable_reward_count",
]

router = APIRouter(prefix="/pendle", tags=["pendle"])


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


def _base_row(
    wallet: str,
    chain_id: int | None,
    portfolio_value: Decimal,
) -> dict[str, str]:
    row = {column: "" for column in PENDLE_CSV_HEADER}
    row.update(
        {
            "wallet": wallet,
            "chain_id": "" if chain_id is None else str(chain_id),
            "chain": (
                ""
                if chain_id is None
                else PENDLE_CHAIN_NAMES.get(chain_id, f"Chain {chain_id}")
            ),
            "protocol": "Pendle",
            "portfolio_value_usd": _format_decimal(portfolio_value),
        }
    )
    return row


def _claim_rows(
    wallet: str,
    chain_id: int,
    portfolio_value: Decimal,
    parent_id: str,
    market_id: str,
    asset_type: str,
    claims: object,
    updated_at: str,
) -> list[dict[str, str]]:
    if not isinstance(claims, list):
        return []
    rows = []
    for claim in claims:
        if not isinstance(claim, dict):
            continue
        token_id = str(claim.get("token") or "")
        amount = str(claim.get("amount") or "0")
        if not token_id or _decimal(amount) == 0:
            continue
        row = _base_row(wallet, chain_id, portfolio_value)
        row.update(
            {
                "position_id": f"{parent_id}:reward:{token_id.lower()}",
                "row_type": "claimable_reward",
                "status": "claimable",
                "market_id": market_id,
                "asset_type": asset_type,
                "claimable_token_id": token_id.lower(),
                "claimable_amount_raw": amount,
                "updated_at": updated_at,
            }
        )
        rows.append(row)
    return rows


def _market_rows(
    wallet: str,
    chain_id: int,
    portfolio_value: Decimal,
    market: dict[str, Any],
    status: str,
    updated_at: str,
) -> list[dict[str, str]]:
    market_id = str(market.get("marketId") or "").lower()
    if not market_id:
        return []
    rows = []
    for field, asset_type in (("pt", "PT"), ("yt", "YT"), ("lp", "LP")):
        position = market.get(field)
        if not isinstance(position, dict):
            continue
        claims = position.get("claimTokenAmounts")
        balance = str(position.get("balance") or "0")
        active_balance = str(position.get("activeBalance") or "0")
        valuation = _decimal(position.get("valuation"))
        if (
            status == "open"
            and _decimal(balance) == 0
            and _decimal(active_balance) == 0
            and valuation == 0
            and not claims
        ):
            continue

        parent_id = f"{market_id}:{field}"
        row = _base_row(wallet, chain_id, portfolio_value)
        row.update(
            {
                "position_id": parent_id,
                "row_type": "position",
                "status": status,
                "market_id": market_id,
                "asset_type": asset_type,
                "balance_raw": balance,
                "active_balance_raw": active_balance if field == "lp" else "",
                "value_usd": _format_decimal(valuation),
                "updated_at": updated_at,
            }
        )
        rows.append(row)
        rows.extend(
            _claim_rows(
                wallet,
                chain_id,
                portfolio_value,
                parent_id,
                market_id,
                asset_type,
                claims,
                updated_at,
            )
        )

    cross_positions = market.get("crossPtPositions")
    if isinstance(cross_positions, list):
        for position in cross_positions:
            if not isinstance(position, dict):
                continue
            spoke_pt = str(position.get("spokePt") or "").lower()
            if not spoke_pt:
                continue
            row = _base_row(wallet, chain_id, portfolio_value)
            row.update(
                {
                    "position_id": f"{market_id}:cross_pt:{spoke_pt}",
                    "row_type": "cross_pt_position",
                    "status": status,
                    "market_id": market_id,
                    "asset_type": "cross-PT",
                    "token_id": spoke_pt,
                    "balance_raw": str(position.get("balance") or "0"),
                    "updated_at": updated_at,
                }
            )
            rows.append(row)
    return rows


def _parse_rows(
    wallet: str,
    payload: object,
    include_closed: bool,
) -> list[dict[str, str]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("positions"), list):
        raise HTTPException(status_code=502, detail="Pendle API returned invalid data")

    chains = [item for item in payload["positions"] if isinstance(item, dict)]
    portfolio_value = sum(
        (
            _decimal(position.get("valuation"))
            for chain in chains
            for market in chain.get("openPositions", [])
            if isinstance(market, dict)
            for position in (market.get("pt"), market.get("yt"), market.get("lp"))
            if isinstance(position, dict)
        ),
        Decimal(0),
    )
    open_count = sum(int(chain.get("totalOpen") or 0) for chain in chains)
    closed_count = sum(int(chain.get("totalClosed") or 0) for chain in chains)
    sy_count = sum(int(chain.get("totalSy") or 0) for chain in chains)

    rows: list[dict[str, str]] = []
    for chain in chains:
        try:
            chain_id = int(chain.get("chainId"))
        except (TypeError, ValueError):
            continue
        updated_at = str(chain.get("updatedAt") or "")
        for market in chain.get("openPositions", []):
            if isinstance(market, dict):
                rows.extend(
                    _market_rows(
                        wallet,
                        chain_id,
                        portfolio_value,
                        market,
                        "open",
                        updated_at,
                    )
                )
        if include_closed:
            for market in chain.get("closedPositions", []):
                if isinstance(market, dict):
                    rows.extend(
                        _market_rows(
                            wallet,
                            chain_id,
                            portfolio_value,
                            market,
                            "closed",
                            updated_at,
                        )
                    )
        for position in chain.get("syPositions", []):
            if not isinstance(position, dict):
                continue
            sy_id = str(position.get("syId") or "").lower()
            if not sy_id:
                continue
            row = _base_row(wallet, chain_id, portfolio_value)
            row.update(
                {
                    "position_id": f"{sy_id}:sy",
                    "row_type": "sy_position",
                    "status": "open",
                    "asset_type": "SY",
                    "token_id": sy_id,
                    "balance_raw": str(position.get("balance") or "0"),
                    "updated_at": updated_at,
                }
            )
            rows.append(row)
            rows.extend(
                _claim_rows(
                    wallet,
                    chain_id,
                    portfolio_value,
                    f"{sy_id}:sy",
                    "",
                    "SY",
                    position.get("claimTokenAmounts"),
                    updated_at,
                )
            )

    rows.sort(key=lambda row: row["position_id"])
    claimable_count = sum(row["row_type"] == "claimable_reward" for row in rows)
    summary = _base_row(wallet, None, portfolio_value)
    summary.update(
        {
            "position_id": f"{wallet}:portfolio",
            "row_type": "portfolio_summary",
            "status": "open",
            "open_market_count": str(open_count),
            "closed_market_count": str(closed_count),
            "sy_position_count": str(sy_count),
            "claimable_reward_count": str(claimable_count),
        }
    )
    return [summary, *rows]


async def _fetch_pendle_rows(
    client: httpx.AsyncClient,
    wallet: str,
    include_closed: bool,
) -> list[dict[str, str]]:
    try:
        response = await client.get(
            f"{PENDLE_API_URL}/v1/dashboard/positions/database/{wallet}",
            params={"filterUsd": 0},
        )
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Pendle API returned HTTP {exc.response.status_code}",
        ) from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Pendle API request failed"
        ) from exc
    return _parse_rows(wallet, payload, include_closed)


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=PENDLE_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export Pendle positions",
    description=(
        "Returns Pendle PT, YT, LP, cross-chain PT, and SY positions across all "
        "supported networks, with USD valuations and claimable reward amounts. "
        "Pendle may cache claimable rewards for up to 24 hours."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_pendle_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    include_closed: bool = Query(
        False,
        description="Include closed Pendle market positions.",
    ),
):
    wallet = _normalize_wallet(address)
    async with queued_async_client(timeout=120.0, trust_env=False) as client:
        rows = await _fetch_pendle_rows(client, wallet, include_closed)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={PENDLE_CACHE_TTL_SECONDS}"},
    )
