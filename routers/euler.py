import csv
import io
import re
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from outbound_queue import queued_async_client


EULER_API_BASE_URL = "https://v3.euler.finance/v3"
EULER_CACHE_TTL_SECONDS = 60
EULER_CHAIN_NAMES = {
    1: "Ethereum",
    10: "Optimism",
    56: "BNB Chain",
    100: "Gnosis",
    130: "Unichain",
    137: "Polygon",
    146: "Sonic",
    239: "TAC",
    999: "HyperEVM",
    1923: "Swell",
    8453: "Base",
    42161: "Arbitrum",
    43114: "Avalanche",
    80094: "Berachain",
}
EULER_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "protocol",
    "position_id",
    "account",
    "vault_address",
    "vault_type",
    "vault_name",
    "vault_symbol",
    "asset_address",
    "asset_symbol",
    "asset_name",
    "asset_decimals",
    "shares_raw",
    "supply_amount",
    "supply_usd",
    "borrow_amount",
    "borrow_usd",
    "market_price_usd",
    "is_collateral",
    "is_controller",
    "liability_value_usd",
    "total_collateral_value_usd",
    "health_factor",
    "days_to_liquidation",
    "snapshot_timestamp",
]

router = APIRouter(prefix="/euler", tags=["euler"])


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


def _text(value: object | None) -> str:
    return "" if value is None else str(value)


def _decimal(value: object | None) -> Decimal:
    try:
        return Decimal(str(value)) if value is not None else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _format(value: Decimal) -> str:
    return "0" if value == 0 else format(value.normalize(), "f")


def _token_amount(raw: object | None, decimals: int) -> str:
    if raw is None:
        return ""
    return _format(_decimal(raw) / (Decimal(10) ** decimals))


def _health_factor(liability_usd: Decimal, collateral_usd: Decimal) -> str:
    if liability_usd <= 0:
        return ""
    return _format(collateral_usd / liability_usd)


def _parse_rows(
    wallet: str, chain_id: int, payload: object
) -> list[dict[str, str]]:
    envelope = _dict(payload)
    data = _dict(envelope.get("data"))
    portfolio = _dict(data.get("portfolio"))
    positions = portfolio.get("positions")
    if not isinstance(positions, list):
        return []

    rows = []
    for value in positions:
        position = _dict(value)
        vault = _dict(position.get("vault"))
        asset = _dict(vault.get("asset"))
        liquidity = _dict(position.get("liquidity"))
        vault_address = _text(
            position.get("vaultAddress") or vault.get("address")
        ).lower()
        account = _text(position.get("account")).lower()
        if not vault_address:
            continue
        decimals = int(asset.get("decimals") or 0)
        supply_amount = _token_amount(position.get("assets"), decimals)
        borrow_amount = _token_amount(position.get("borrowed"), decimals)
        supply_usd = _decimal(position.get("suppliedValueUsd"))
        borrow_usd = _decimal(position.get("borrowedValueUsd"))
        liability_usd = _decimal(liquidity.get("liabilityValueUsd"))
        collateral_usd = _decimal(liquidity.get("totalCollateralValueUsd"))
        if (
            _decimal(supply_amount) == 0
            and _decimal(borrow_amount) == 0
            and supply_usd == 0
            and borrow_usd == 0
        ):
            continue
        row = {column: "" for column in EULER_CSV_HEADER}
        row.update(
            {
                "wallet": wallet,
                "chain_id": str(chain_id),
                "chain": EULER_CHAIN_NAMES.get(chain_id, f"Chain {chain_id}"),
                "protocol": "Euler V3",
                "position_id": f"{chain_id}:{account}:{vault_address}",
                "account": account,
                "vault_address": vault_address,
                "vault_type": _text(vault.get("type")),
                "vault_name": _text(vault.get("name")),
                "vault_symbol": _text(vault.get("symbol")),
                "asset_address": _text(asset.get("address")).lower(),
                "asset_symbol": _text(asset.get("symbol")),
                "asset_name": _text(asset.get("name")),
                "asset_decimals": str(decimals),
                "shares_raw": _text(position.get("shares")),
                "supply_amount": supply_amount,
                "supply_usd": _format(supply_usd),
                "borrow_amount": borrow_amount,
                "borrow_usd": _format(borrow_usd),
                "market_price_usd": _text(position.get("marketPriceUsd")),
                "is_collateral": _text(position.get("isCollateral")).lower(),
                "is_controller": _text(position.get("isController")).lower(),
                "liability_value_usd": _format(liability_usd),
                "total_collateral_value_usd": _format(collateral_usd),
                "health_factor": _health_factor(liability_usd, collateral_usd),
                "days_to_liquidation": _text(
                    liquidity.get("daysToLiquidation")
                ),
                "snapshot_timestamp": _text(
                    _dict(position.get("snapshot")).get("timestamp")
                ),
            }
        )
        rows.append(row)
    return sorted(rows, key=lambda row: row["position_id"])


async def _fetch_euler_rows(
    client: httpx.AsyncClient, wallet: str, chain_id: int
) -> list[dict[str, str]]:
    try:
        response = await client.get(
            f"{EULER_API_BASE_URL}/accounts/{wallet}/portfolio",
            params={"chainId": chain_id},
        )
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPStatusError as exc:
        detail = "Euler API request failed"
        try:
            error = _dict(exc.response.json().get("error"))
            detail = _text(error.get("message")) or detail
        except (AttributeError, ValueError):
            pass
        raise HTTPException(status_code=502, detail=detail) from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(status_code=502, detail="Euler API request failed") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="Euler API returned invalid data")
    return _parse_rows(wallet, chain_id, payload)


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=EULER_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export Euler positions",
    description=(
        "Returns Euler V3 supply, borrow, collateral, valuation, and risk data "
        "from the official public portfolio API."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_euler_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(1, ge=1, description="EVM chain ID."),
):
    wallet = _normalize_wallet(address)
    async with queued_async_client(timeout=30.0, trust_env=False) as client:
        rows = await _fetch_euler_rows(client, wallet, chain_id)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={EULER_CACHE_TTL_SECONDS}"},
    )
