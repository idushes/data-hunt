import time

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from config import (
    COINMARKETCAP_API_KEY,
    COINMARKETCAP_BASE_URL,
    COINMARKETCAP_CACHE_TTL_SECONDS,
)


router = APIRouter(prefix="/cmc", tags=["cmc"])

_price_cache: dict[tuple[str, str], tuple[float, float]] = {}


def _get_cached_price(cache_key: tuple[str, str]) -> float | None:
    cached = _price_cache.get(cache_key)
    if not cached:
        return None

    expires_at, price = cached
    if expires_at <= time.time():
        _price_cache.pop(cache_key, None)
        return None

    return price


def _set_cached_price(cache_key: tuple[str, str], price: float) -> None:
    _price_cache[cache_key] = (time.time() + COINMARKETCAP_CACHE_TTL_SECONDS, price)


async def _fetch_price_from_cmc(
    symbol: str | None, token_id: int | None, convert: str
) -> float:
    if not COINMARKETCAP_API_KEY:
        raise HTTPException(
            status_code=500, detail="COINMARKETCAP_API_KEY is not configured"
        )

    params = {"convert": convert}
    if token_id is not None:
        params["id"] = str(token_id)
    else:
        params["symbol"] = symbol

    headers = {
        "Accept": "application/json",
        "X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY,
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            f"{COINMARKETCAP_BASE_URL}/v2/cryptocurrency/quotes/latest",
            params=params,
            headers=headers,
        )

    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="CoinMarketCap request failed")

    payload = response.json()
    data = payload.get("data") or {}

    if token_id is not None:
        token_data = data.get(str(token_id))
    else:
        token_rows = data.get(symbol.upper()) or []
        token_data = token_rows[0] if token_rows else None

    if not token_data:
        raise HTTPException(status_code=404, detail="Token not found in CoinMarketCap")

    quote = token_data.get("quote", {}).get(convert)
    if not quote or quote.get("price") is None:
        raise HTTPException(status_code=404, detail=f"Price in {convert} not found")

    return float(quote["price"])


@router.get(
    "/price.csv",
    summary="Get token price for Google Sheets",
    description="Returns a single-cell CSV value with the latest token price from CoinMarketCap. Pass either `id` or `symbol`.",
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "Single numeric value suitable for IMPORTDATA.",
        }
    },
)
async def get_price_csv(
    symbol: str | None = Query(
        default=None, description="CoinMarketCap symbol, for example BTC or ETH"
    ),
    id: int | None = Query(default=None, description="CoinMarketCap numeric asset id"),
    convert: str = Query(default="USD", description="Quote currency, for example USD"),
):
    if id is None and not symbol:
        raise HTTPException(
            status_code=400, detail="Either 'id' or 'symbol' is required"
        )

    convert = convert.upper().strip()
    symbol = symbol.upper().strip() if symbol else None
    cache_key = (f"id:{id}" if id is not None else f"symbol:{symbol}", convert)

    price = _get_cached_price(cache_key)
    if price is None:
        price = await _fetch_price_from_cmc(symbol=symbol, token_id=id, convert=convert)
        _set_cached_price(cache_key, price)

    return Response(content=str(price), media_type="text/csv")
