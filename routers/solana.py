import csv
import io
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response


GRAPHQL_ENDPOINT = "https://gmx-solana-sqd.squids.live/gmx-solana-base:prod/api/graphql"
SOLANA_RPC_ENDPOINT = "https://api.mainnet-beta.solana.com"

router = APIRouter(prefix="/solana", tags=["solana"])


async def _query_graphql(client: httpx.AsyncClient, query: str) -> dict[str, Any]:
    response = await client.post(
        GRAPHQL_ENDPOINT,
        json={"query": query},
        headers={"Content-Type": "application/json"},
    )

    if response.status_code < 200 or response.status_code >= 300:
        raise HTTPException(
            status_code=502,
            detail=f"GMTrade GraphQL request failed with status {response.status_code}",
        )

    try:
        payload = response.json()
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail="GMTrade returned invalid JSON"
        ) from exc

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502, detail="Unexpected GMTrade response format"
        )

    errors = payload.get("errors") or []
    if errors:
        first_error = errors[0] if isinstance(errors[0], dict) else {}
        message = first_error.get("message") or "GMTrade query failed"
        raise HTTPException(status_code=502, detail=message)

    data = payload.get("data")
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="GMTrade response is missing data")

    return data


def _quote_list(values: list[str]) -> str:
    return ",".join(f'"{value}"' for value in values)


def _decimal_1e9(raw: str | None) -> float:
    if not raw:
        return 0.0
    try:
        return float(raw) / 1e9
    except (TypeError, ValueError):
        return 0.0


def _decimal_1e11(raw: str | None) -> float:
    if not raw:
        return 0.0
    try:
        return float(raw) / 1e11
    except (TypeError, ValueError):
        return 0.0


def _round2(value: float) -> float:
    return round(value, 2)


def _round9(value: float) -> float:
    return round(value, 9)


def _fallback_name(name: str | None, mint: str) -> str:
    return name or mint


def _first_non_empty(*values: str | None) -> str:
    for value in values:
        if value:
            return value
    return ""


def _collect_unique_mints(items: list[dict[str, Any]], key: str) -> list[str]:
    seen = set()
    result = []
    for item in items:
        mint = item.get(key)
        if not mint or mint in seen:
            continue
        seen.add(mint)
        result.append(mint)
    return result


async def _fetch_market_gm_users(
    client: httpx.AsyncClient, wallet: str
) -> list[dict[str, Any]]:
    data = await _query_graphql(
        client,
        f'{{ marketGmUsers(where:{{owner_eq:"{wallet}"}}) {{ owner marketToken balance factor settledFees accruedFees timestamp }} }}',
    )
    users = data.get("marketGmUsers") or []
    return users if isinstance(users, list) else []


async def _fetch_glv_users(
    client: httpx.AsyncClient, wallet: str
) -> list[dict[str, Any]]:
    data = await _query_graphql(
        client,
        f'{{ glvUsers(where:{{owner_eq:"{wallet}"}}) {{ owner glvToken balance factor settledFees accruedFees timestamp }} }}',
    )
    users = data.get("glvUsers") or []
    return users if isinstance(users, list) else []


async def _fetch_market_infos(client: httpx.AsyncClient) -> dict[str, dict[str, Any]]:
    data = await _query_graphql(
        client,
        "{ marketInfos { id name longTokenMint shortTokenMint indexTokenMint decimal } }",
    )
    items = data.get("marketInfos") or []
    if not isinstance(items, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        item_id = item.get("id")
        if isinstance(item_id, str) and item_id:
            result[item_id] = item
    return result


async def _fetch_market_gm_infos(
    client: httpx.AsyncClient, mints: list[str]
) -> dict[str, dict[str, Any]]:
    if not mints:
        return {}

    data = await _query_graphql(
        client,
        f"{{ marketGmInfos(where:{{id_in:[{_quote_list(mints)}]}}) {{ id supply gmPriceNow apy pnlApy timestamp }} }}",
    )
    items = data.get("marketGmInfos") or []
    if not isinstance(items, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        item_id = item.get("id")
        if isinstance(item_id, str) and item_id:
            result[item_id] = item
    return result


async def _fetch_glv_infos(
    client: httpx.AsyncClient, mints: list[str]
) -> dict[str, dict[str, Any]]:
    if not mints:
        return {}

    data = await _query_graphql(
        client,
        f"{{ glvInfos(where:{{id_in:[{_quote_list(mints)}]}}) {{ id supply glvPriceNow apy timestamp }} }}",
    )
    items = data.get("glvInfos") or []
    if not isinstance(items, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        item_id = item.get("id")
        if isinstance(item_id, str) and item_id:
            result[item_id] = item
    return result


async def _fetch_asset_name(client: httpx.AsyncClient, mint: str) -> str:
    response = await client.post(
        SOLANA_RPC_ENDPOINT,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAsset",
            "params": {"id": mint},
        },
        headers={"Content-Type": "application/json"},
    )

    if response.status_code < 200 or response.status_code >= 300:
        raise HTTPException(
            status_code=502,
            detail=f"Solana RPC request failed with status {response.status_code}",
        )

    try:
        payload = response.json()
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail="Solana RPC returned invalid JSON"
        ) from exc

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502, detail="Unexpected Solana RPC response format"
        )

    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message") or "Solana RPC returned an error"
        raise HTTPException(status_code=502, detail=message)

    result = payload.get("result") or {}
    content = result.get("content") if isinstance(result, dict) else {}
    metadata = content.get("metadata") if isinstance(content, dict) else {}
    name = metadata.get("name") if isinstance(metadata, dict) else ""
    return name if isinstance(name, str) else ""


async def _fetch_asset_names(
    client: httpx.AsyncClient, mints: list[str]
) -> dict[str, str]:
    names = {}
    for mint in mints:
        names[mint] = await _fetch_asset_name(client, mint)
    return names


def _build_gm_rows(
    users: list[dict[str, Any]],
    market_infos: dict[str, dict[str, Any]],
    gm_infos: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for user in users:
        mint = user.get("marketToken")
        if not mint:
            continue

        market = market_infos.get(mint, {})
        gm_info = gm_infos.get(mint, {})
        balance = _decimal_1e9(user.get("balance"))
        price = _decimal_1e11(gm_info.get("gmPriceNow"))

        rows.append(
            {
                "type": "GM",
                "mint": mint,
                "name": _fallback_name(market.get("name"), mint),
                "balance": _round9(balance),
                "price_usd": _round9(price),
                "value_usd": _round2(balance * price),
                "long_token_mint": market.get("longTokenMint", ""),
                "short_token_mint": market.get("shortTokenMint", ""),
                "index_token_mint": market.get("indexTokenMint", ""),
                "updated_at": _first_non_empty(
                    gm_info.get("timestamp"), user.get("timestamp")
                ),
            }
        )

    rows.sort(key=lambda item: item["value_usd"], reverse=True)
    return rows


def _build_glv_rows(
    users: list[dict[str, Any]],
    glv_infos: dict[str, dict[str, Any]],
    asset_names: dict[str, str],
) -> list[dict[str, Any]]:
    rows = []
    for user in users:
        mint = user.get("glvToken")
        if not mint:
            continue

        info = glv_infos.get(mint, {})
        balance = _decimal_1e9(user.get("balance"))
        price = _decimal_1e11(info.get("glvPriceNow"))

        rows.append(
            {
                "type": "GLV",
                "mint": mint,
                "name": _fallback_name(asset_names.get(mint), mint),
                "balance": _round9(balance),
                "price_usd": _round9(price),
                "value_usd": _round2(balance * price),
                "long_token_mint": "",
                "short_token_mint": "",
                "index_token_mint": "",
                "updated_at": _first_non_empty(
                    info.get("timestamp"), user.get("timestamp")
                ),
            }
        )

    rows.sort(key=lambda item: item["value_usd"], reverse=True)
    return rows


@router.get(
    "/gmtrade.csv",
    summary="Export Solana GMTrade assets for Google Sheets",
    description=(
        "Returns a CSV table with GM and GLV positions for a Solana wallet address. "
        "Suitable for Google Sheets IMPORTDATA."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_gmtrade_csv(
    wallet: str = Query(..., description="Solana wallet address"),
):
    normalized_wallet = wallet.strip()
    if not normalized_wallet:
        raise HTTPException(status_code=400, detail="wallet is required")

    async with httpx.AsyncClient(timeout=20.0) as client:
        gm_users = await _fetch_market_gm_users(client, normalized_wallet)
        glv_users = await _fetch_glv_users(client, normalized_wallet)
        market_infos = await _fetch_market_infos(client)
        gm_infos = await _fetch_market_gm_infos(
            client, _collect_unique_mints(gm_users, "marketToken")
        )
        glv_mints = _collect_unique_mints(glv_users, "glvToken")
        glv_infos = await _fetch_glv_infos(client, glv_mints)
        asset_names = await _fetch_asset_names(client, glv_mints)

    rows = _build_gm_rows(gm_users, market_infos, gm_infos) + _build_glv_rows(
        glv_users, glv_infos, asset_names
    )
    rows.sort(key=lambda item: item["value_usd"], reverse=True)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "type",
            "mint",
            "name",
            "balance",
            "price_usd",
            "value_usd",
            "long_token_mint",
            "short_token_mint",
            "index_token_mint",
            "updated_at",
        ]
    )

    for row in rows:
        writer.writerow(
            [
                row["type"],
                row["mint"],
                row["name"],
                row["balance"],
                row["price_usd"],
                row["value_usd"],
                row["long_token_mint"],
                row["short_token_mint"],
                row["index_token_mint"],
                row["updated_at"],
            ]
        )

    return Response(content=output.getvalue(), media_type="text/csv")
