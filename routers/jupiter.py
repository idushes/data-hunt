import asyncio
import csv
import io
import os
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from outbound_queue import queued_async_client
from routers.solana import SOLANA_RPC_ENDPOINT, _is_solana_address, _solana_rpc_request


JUPITER_CACHE_TTL_SECONDS = 60
JUPITER_JLP_INFO_URL = "https://perps-api.jup.ag/v1/jlp-info"
JLP_MINT = "27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4"
JLP_DECIMALS = 6
JUPITER_JLP_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "protocol",
    "position_id",
    "token_mint",
    "token_symbol",
    "balance",
    "price_usd",
    "value_usd",
    "apr_percent",
    "apy_percent",
    "pool_aum_usd",
    "pool_aum_limit_usd",
    "total_supply",
    "apr_updated_timestamp",
    "realized_fee_usd",
]

router = APIRouter(prefix="/jupiter", tags=["jupiter"])


def _normalize_wallet(wallet: str) -> str:
    normalized = wallet.strip()
    if not _is_solana_address(normalized):
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")
    return normalized


def _decimal(value: object | None) -> Decimal:
    try:
        return Decimal(str(value)) if value is not None else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _format(value: Decimal) -> str:
    return "0" if value == 0 else format(value.normalize(), "f")


def _sum_token_accounts(result: object) -> int:
    data = result if isinstance(result, dict) else {}
    values = data.get("value")
    if not isinstance(values, list):
        raise HTTPException(
            status_code=502, detail="Solana RPC returned invalid token accounts"
        )
    total = 0
    for value in values:
        account = value.get("account", {}) if isinstance(value, dict) else {}
        parsed = account.get("data", {}).get("parsed", {})
        amount = parsed.get("info", {}).get("tokenAmount", {}).get("amount")
        try:
            total += int(amount)
        except (TypeError, ValueError):
            continue
    return total


async def _fetch_jlp_balance(
    client: httpx.AsyncClient, wallet: str
) -> Decimal:
    rpc_url = os.getenv("JUPITER_SOLANA_RPC_URL") or SOLANA_RPC_ENDPOINT
    result = await _solana_rpc_request(
        client,
        "getTokenAccountsByOwner",
        [
            wallet,
            {"mint": JLP_MINT},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
        endpoint=rpc_url,
    )
    return Decimal(_sum_token_accounts(result)) / (Decimal(10) ** JLP_DECIMALS)


async def _fetch_jlp_info(client: httpx.AsyncClient) -> dict[str, Any]:
    try:
        response = await client.get(JUPITER_JLP_INFO_URL)
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Jupiter Perps API request failed"
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502, detail="Jupiter Perps API returned invalid data"
        )
    return payload


async def _fetch_jlp_rows(
    client: httpx.AsyncClient, wallet: str
) -> list[dict[str, str]]:
    balance, info = await asyncio.gather(
        _fetch_jlp_balance(client, wallet), _fetch_jlp_info(client)
    )
    price = _decimal(info.get("jlpPriceUsdFormatted"))
    return [
        {
            "wallet": wallet,
            "chain_id": "solana-mainnet",
            "chain": "Solana",
            "protocol": "Jupiter Perps",
            "position_id": "jlp",
            "token_mint": JLP_MINT,
            "token_symbol": "JLP",
            "balance": _format(balance),
            "price_usd": _format(price),
            "value_usd": _format(balance * price),
            "apr_percent": str(info.get("jlpAprPct") or ""),
            "apy_percent": str(info.get("jlpApyPct") or ""),
            "pool_aum_usd": str(info.get("aumUsdFormatted") or ""),
            "pool_aum_limit_usd": str(info.get("aumLimitUsdFormatted") or ""),
            "total_supply": str(info.get("jlpTotalSupplyFormatted") or ""),
            "apr_updated_timestamp": str(
                info.get("jlpAprLastUpdatedTimestamp") or ""
            ),
            "realized_fee_usd": _format(
                _decimal(info.get("jlpRealizedFeeUsd")) / Decimal(10**6)
            ),
        }
    ]


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=JUPITER_JLP_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/jlp.csv",
    summary="Export a Jupiter JLP position",
    description=(
        "Returns a Solana wallet's JLP balance plus the official Jupiter Perps "
        "price, USD value, APR, APY, AUM, supply, and realized fees."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_jupiter_jlp_csv(
    wallet: str = Query(..., description="Solana wallet address."),
):
    normalized_wallet = _normalize_wallet(wallet)
    async with queued_async_client(timeout=30.0, trust_env=False) as client:
        rows = await _fetch_jlp_rows(client, normalized_wallet)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={JUPITER_CACHE_TTL_SECONDS}"},
    )
