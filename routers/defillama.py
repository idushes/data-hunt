"""DefiLlama USD prices behind the existing Sheets route and resource IDs."""

import asyncio
import math
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from urllib.parse import quote

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from config import DEFILLAMA_BASE_URL, DEFILLAMA_CACHE_TTL_SECONDS
from csv_cache import DATA_UPDATED_AT_HEADER
from outbound_queue import queued_async_client


# Keep the published URL and the "cmc-price" resource ID so existing formulas
# and database fingerprints continue to resolve without a data migration.
router = APIRouter(prefix="/cmc", tags=["prices"])

TICKER_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "WETH": "weth",
    "WBTC": "wrapped-bitcoin",
    "SOL": "solana",
    "BNB": "binancecoin",
    "AVAX": "avalanche-2",
    "ARB": "arbitrum",
    "OP": "optimism",
    "USDC": "usd-coin",
    "USDT": "tether",
    "DAI": "dai",
    "PAXG": "pax-gold",
    "STETH": "staked-ether",
    "WSTETH": "wrapped-steth",
    "MON": "monad",
}
# Explicit migration aliases, never interpret a CMC numeric ID as a different
# provider's ID. Other assets are addressable through the new `coin` parameter.
LEGACY_CMC_IDS = {
    1: "bitcoin",
    1027: "ethereum",
    5426: "solana",
    4705: "pax-gold",
    12409: "wrapped-steth",
    30495: "monad",
}
MAX_PRICE_AGE_SECONDS = 3600
MAX_CACHE_ENTRIES = 256


@dataclass(frozen=True)
class Price:
    value: float
    timestamp: int


_price_cache: OrderedDict[str, tuple[float, Price]] = OrderedDict()
_inflight: dict[str, asyncio.Task[Price]] = {}


def _normalize_coin(value: str) -> str:
    value = value.strip()
    if not value or len(value) > 256:
        raise HTTPException(status_code=400, detail="A token identifier is required")
    if ":" not in value:
        value = f"coingecko:{TICKER_IDS.get(value.upper(), value.lower())}"
    chain, _, identifier = value.partition(":")
    chain = chain.lower()
    if chain == "coingecko":
        identifier = identifier.lower()
        valid = re.fullmatch(r"[a-z0-9][a-z0-9-]{0,127}", identifier)
        valid = valid and not identifier.isdigit()
    else:
        valid = re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", chain)
        valid = valid and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,191}", identifier)
        if identifier.lower().startswith("0x"):
            valid = valid and re.fullmatch(r"0x[0-9a-fA-F]{40}", identifier)
            identifier = identifier.lower()
    if not valid:
        raise HTTPException(
            status_code=400,
            detail="Use a CoinGecko ID such as pax-gold or a chain:contract address",
        )
    # Non-EVM addresses (including Solana mints) are case-sensitive.
    return f"{chain}:{identifier}"


def _resolve_coin(coin: str | None, symbol: str | None, token_id: int | None) -> str:
    if coin and coin.strip():
        return _normalize_coin(coin)
    if token_id is not None:
        identifier = LEGACY_CMC_IDS.get(token_id)
        if identifier is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "This legacy CoinMarketCap ID needs a DefiLlama coin identifier; "
                    "use coin=pax-gold or chain:contract"
                ),
            )
        return f"coingecko:{identifier}"
    if symbol and symbol.strip():
        identifier = TICKER_IDS.get(symbol.strip().upper())
        if identifier is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Unknown ticker; use coin with an exact CoinGecko ID "
                    "or chain:contract address"
                ),
            )
        return f"coingecko:{identifier}"
    raise HTTPException(status_code=400, detail="A coin identifier or ticker is required")


def _valid_price(price: Price) -> bool:
    age = time.time() - price.timestamp
    return -60 <= age < MAX_PRICE_AGE_SECONDS


def _cached_price(coin: str) -> Price | None:
    cached = _price_cache.get(coin)
    if cached is None:
        return None
    expires_at, price = cached
    if expires_at <= time.monotonic() or not _valid_price(price):
        _price_cache.pop(coin, None)
        return None
    _price_cache.move_to_end(coin)
    return price


def _finite_number(value: object) -> bool:
    if not isinstance(value, (float, int)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


async def _fetch_price(coin: str) -> Price:
    try:
        async with queued_async_client(timeout=15.0) as client:
            response = await client.get(
                f"{DEFILLAMA_BASE_URL.rstrip('/')}/prices/current/{quote(coin, safe=':')}",
                headers={"Accept": "application/json"},
            )
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail="DefiLlama price request timed out") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="DefiLlama is unavailable") from exc
    if response.status_code == 429:
        raise HTTPException(
            status_code=503, detail="DefiLlama rate limit reached; try again later"
        )
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="DefiLlama price request failed")
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Invalid DefiLlama price response") from exc
    coins = payload.get("coins") if isinstance(payload, dict) else None
    if not isinstance(coins, dict):
        raise HTTPException(status_code=502, detail="Invalid DefiLlama price response")
    row = coins.get(coin)
    if row is None:
        raise HTTPException(status_code=404, detail="Token price not found in DefiLlama")
    if not isinstance(row, dict):
        raise HTTPException(status_code=502, detail="Invalid DefiLlama price response")
    value, timestamp = row.get("price"), row.get("timestamp")
    confidence = row.get("confidence")
    if not _finite_number(value) or value <= 0 or not _finite_number(timestamp):
        raise HTTPException(status_code=502, detail="Invalid DefiLlama price response")
    if confidence is not None and (
        not _finite_number(confidence) or not 0.5 <= confidence <= 1
    ):
        raise HTTPException(status_code=503, detail="DefiLlama price confidence is too low")
    price = Price(float(value), int(timestamp))
    if not _valid_price(price):
        raise HTTPException(
            status_code=503,
            detail="DefiLlama has no price updated within the last hour",
        )
    ttl = min(
        DEFILLAMA_CACHE_TTL_SECONDS,
        MAX_PRICE_AGE_SECONDS - (time.time() - price.timestamp),
    )
    _price_cache[coin] = (time.monotonic() + ttl, price)
    _price_cache.move_to_end(coin)
    while len(_price_cache) > MAX_CACHE_ENTRIES:
        _price_cache.popitem(last=False)
    return price


async def _get_price(coin: str) -> Price:
    cached = _cached_price(coin)
    if cached is not None:
        return cached
    task = _inflight.get(coin)
    if task is None:
        task = asyncio.create_task(_fetch_price(coin))
        _inflight[coin] = task

        def completed(done: asyncio.Task[Price]) -> None:
            if _inflight.get(coin) is done:
                _inflight.pop(coin, None)
            if not done.cancelled():
                done.exception()  # Retrieve failures even if every waiter disconnected.

        task.add_done_callback(completed)
    return await asyncio.shield(task)


@router.get(
    "/price.csv",
    summary="Get a DefiLlama USD token price for Google Sheets",
    description=(
        "Returns a single-cell USD price from DefiLlama without a provider API key. "
        "Use coin with a CoinGecko ID or chain:contract address. The legacy URL, "
        "supported ticker aliases, and selected numeric IDs remain compatible "
        "with existing Sheets links. Data Hunt authentication is still required."
    ),
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "One USD price, with its source timestamp in X-Data-Updated-At.",
        }
    },
)
async def get_price_csv(
    coin: str | None = Query(
        default=None,
        max_length=256,
        description=(
            "Token identifier, e.g. pax-gold, coingecko:bitcoin, or ethereum:0x...; "
            "takes precedence over legacy selectors"
        ),
    ),
    symbol: str | None = Query(
        default=None,
        max_length=128,
        description="Legacy ticker alias, e.g. BTC, ETH, SOL, PAXG, MON (Monad), or WSTETH",
    ),
    id: int | None = Query(
        default=None,
        description="Legacy CMC ID: 1, 1027, 5426, 4705, 12409, or 30495",
        deprecated=True,
    ),
    convert: str = Query(
        default="USD",
        max_length=16,
        description="Quote currency; DefiLlama supports USD",
    ),
):
    if convert.strip().upper() != "USD":
        raise HTTPException(status_code=400, detail="DefiLlama prices are available in USD only")
    identifier = _resolve_coin(coin, symbol, id)
    price = await _get_price(identifier)
    return Response(
        content=str(price.value),
        media_type="text/csv",
        headers={DATA_UPDATED_AT_HEADER: str(price.timestamp)},
    )
