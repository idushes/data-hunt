import csv
import io
import re
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from outbound_queue import queued_async_client


MORPHO_API_URL = "https://api.morpho.org/graphql"
MORPHO_CACHE_TTL_SECONDS = 60
MORPHO_CHAIN_NAMES = {
    1: "Ethereum",
    10: "Optimism",
    130: "Unichain",
    137: "Polygon",
    143: "Monad",
    480: "World Chain",
    999: "HyperEVM",
    4217: "Tempo",
    8453: "Base",
    42161: "Arbitrum",
    747474: "Katana",
}
MORPHO_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "protocol",
    "position_type",
    "position_id",
    "market_id",
    "vault_address",
    "vault_name",
    "vault_symbol",
    "asset_address",
    "asset_symbol",
    "asset_name",
    "collateral_address",
    "collateral_symbol",
    "collateral_name",
    "supply_amount",
    "supply_usd",
    "supply_apy_percent",
    "net_apy_percent",
    "borrow_amount",
    "borrow_usd",
    "borrow_apy_percent",
    "collateral_amount",
    "collateral_usd",
    "ltv_percent",
    "liquidation_threshold_percent",
    "shares",
    "net_usd",
]

MORPHO_POSITIONS_QUERY = """
query MorphoPositions($address: String!, $chainId: Int!) {
  userByAddress(address: $address, chainId: $chainId) {
    address
    marketPositions {
      market {
        marketId
        lltv
        loanAsset { address symbol name decimals }
        collateralAsset { address symbol name decimals }
        state { supplyApy borrowApy }
      }
      state {
        supplyAssets
        supplyAssetsUsd
        borrowAssets
        borrowAssetsUsd
        collateral
        collateralUsd
      }
    }
    vaultPositions {
      vault {
        address
        name
        symbol
        asset { address symbol name decimals }
        state { netApy }
      }
      state { assets assetsUsd shares }
    }
    vaultV2Positions {
      vault {
        address
        name
        symbol
        asset { address symbol name decimals }
        netApy
      }
      assets
      assetsUsd
      shares
    }
  }
}
"""

router = APIRouter(prefix="/morpho", tags=["morpho"])


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


def _format_decimal(value: Decimal) -> str:
    return "0" if value == 0 else format(value.normalize(), "f")


def _token_amount(value: object | None, decimals: object | None) -> Decimal:
    amount = _decimal(value)
    token_decimals = int(_decimal(decimals))
    if token_decimals < 0 or token_decimals > 255:
        return amount
    with localcontext() as context:
        context.prec = max(78, len(amount.as_tuple().digits) + token_decimals)
        return amount / (Decimal(10) ** token_decimals)


def _apy_percent(value: object | None) -> str:
    return _format_decimal(_decimal(value) * Decimal(100))


def _wad_percent(value: object | None) -> str:
    return _format_decimal(_decimal(value) / Decimal("1e16"))


def _ratio_percent(numerator: Decimal, denominator: Decimal) -> str:
    if denominator <= 0:
        return ""
    with localcontext() as context:
        context.prec = 28
        return _format_decimal(numerator / denominator * Decimal(100))


def _base_row(wallet: str, chain_id: int) -> dict[str, str]:
    row = {column: "" for column in MORPHO_CSV_HEADER}
    row.update(
        {
            "wallet": wallet,
            "chain_id": str(chain_id),
            "chain": MORPHO_CHAIN_NAMES.get(chain_id, f"Chain {chain_id}"),
            "protocol": "Morpho",
        }
    )
    return row


def _asset_fields(asset: dict[str, Any]) -> dict[str, str]:
    return {
        "asset_address": _text(asset.get("address")).lower(),
        "asset_symbol": _text(asset.get("symbol")),
        "asset_name": _text(asset.get("name")),
    }


def _parse_market_rows(
    wallet: str, chain_id: int, positions: object
) -> list[dict[str, str]]:
    if not isinstance(positions, list):
        return []
    rows = []
    for item in positions:
        position = _dict(item)
        market = _dict(position.get("market"))
        state = _dict(position.get("state"))
        market_id = _text(market.get("marketId")).lower()
        active = any(
            _decimal(state.get(field)) != 0
            for field in ("supplyAssets", "borrowAssets", "collateral")
        )
        if not market_id or not active:
            continue

        loan_asset = _dict(market.get("loanAsset"))
        collateral_asset = _dict(market.get("collateralAsset"))
        market_state = _dict(market.get("state"))
        supply_usd = _decimal(state.get("supplyAssetsUsd"))
        borrow_usd = _decimal(state.get("borrowAssetsUsd"))
        collateral_usd = _decimal(state.get("collateralUsd"))
        supply_amount = _token_amount(
            state.get("supplyAssets"), loan_asset.get("decimals")
        )
        borrow_amount = _token_amount(
            state.get("borrowAssets"), loan_asset.get("decimals")
        )
        collateral_amount = _token_amount(
            state.get("collateral"), collateral_asset.get("decimals")
        )
        row = _base_row(wallet, chain_id)
        row.update(
            {
                "position_type": "market",
                "position_id": f"market:{chain_id}:{market_id}",
                "market_id": market_id,
                **_asset_fields(loan_asset),
                "collateral_address": _text(
                    collateral_asset.get("address")
                ).lower(),
                "collateral_symbol": _text(collateral_asset.get("symbol")),
                "collateral_name": _text(collateral_asset.get("name")),
                "supply_amount": _format_decimal(supply_amount),
                "supply_usd": _format_decimal(supply_usd),
                "supply_apy_percent": _apy_percent(
                    market_state.get("supplyApy")
                ),
                "borrow_amount": _format_decimal(borrow_amount),
                "borrow_usd": _format_decimal(borrow_usd),
                "borrow_apy_percent": _apy_percent(
                    market_state.get("borrowApy")
                ),
                "collateral_amount": _format_decimal(collateral_amount),
                "collateral_usd": _format_decimal(collateral_usd),
                "ltv_percent": _ratio_percent(borrow_usd, collateral_usd),
                "liquidation_threshold_percent": _wad_percent(
                    market.get("lltv")
                ),
                "net_usd": _format_decimal(
                    supply_usd + collateral_usd - borrow_usd
                ),
            }
        )
        rows.append(row)
    return rows


def _parse_vault_rows(
    wallet: str,
    chain_id: int,
    positions: object,
    version: str,
) -> list[dict[str, str]]:
    if not isinstance(positions, list):
        return []
    rows = []
    for item in positions:
        position = _dict(item)
        vault = _dict(position.get("vault"))
        state = _dict(position.get("state")) if version == "v1" else position
        assets = _decimal(state.get("assets"))
        if assets == 0:
            continue
        address = _text(vault.get("address")).lower()
        if not address:
            continue
        assets_usd = _decimal(state.get("assetsUsd"))
        asset = _dict(vault.get("asset"))
        display_assets = _token_amount(assets, asset.get("decimals"))
        net_apy = (
            _dict(vault.get("state")).get("netApy")
            if version == "v1"
            else vault.get("netApy")
        )
        row = _base_row(wallet, chain_id)
        row.update(
            {
                "position_type": f"vault_{version}",
                "position_id": f"vault-{version}:{chain_id}:{address}",
                "vault_address": address,
                "vault_name": _text(vault.get("name")),
                "vault_symbol": _text(vault.get("symbol")),
                **_asset_fields(asset),
                "supply_amount": _format_decimal(display_assets),
                "supply_usd": _format_decimal(assets_usd),
                "supply_apy_percent": _apy_percent(net_apy),
                "net_apy_percent": _apy_percent(net_apy),
                "shares": _text(state.get("shares")),
                "net_usd": _format_decimal(assets_usd),
            }
        )
        rows.append(row)
    return rows


def _parse_rows(
    wallet: str, chain_id: int, payload: object
) -> list[dict[str, str]]:
    data = _dict(payload)
    user = _dict(data.get("userByAddress"))
    rows = [
        *_parse_market_rows(wallet, chain_id, user.get("marketPositions")),
        *_parse_vault_rows(
            wallet, chain_id, user.get("vaultPositions"), "v1"
        ),
        *_parse_vault_rows(
            wallet, chain_id, user.get("vaultV2Positions"), "v2"
        ),
    ]
    return sorted(rows, key=lambda row: row["position_id"])


async def _fetch_morpho_rows(
    client: httpx.AsyncClient, wallet: str, chain_id: int
) -> list[dict[str, str]]:
    try:
        response = await client.post(
            MORPHO_API_URL,
            json={
                "query": MORPHO_POSITIONS_QUERY,
                "variables": {"address": wallet, "chainId": chain_id},
            },
        )
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Morpho API returned HTTP {exc.response.status_code}",
        ) from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(status_code=502, detail="Morpho API request failed") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="Morpho API returned invalid data")
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        message = _text(_dict(errors[0]).get("message")) or "GraphQL error"
        raise HTTPException(status_code=502, detail=f"Morpho API error: {message}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="Morpho API response is missing data")
    return _parse_rows(wallet, chain_id, data)


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=MORPHO_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export Morpho positions",
    description=(
        "Returns active Morpho market, MetaMorpho vault, and Morpho Vault V2 "
        "positions for one wallet and chain."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_morpho_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(1, ge=1, description="EVM chain ID."),
):
    wallet = _normalize_wallet(address)
    async with queued_async_client(timeout=30.0, trust_env=False) as client:
        rows = await _fetch_morpho_rows(client, wallet, chain_id)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={MORPHO_CACHE_TTL_SECONDS}"},
    )
