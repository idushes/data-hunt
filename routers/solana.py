import asyncio
import base64
import csv
import hashlib
import io
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response


GRAPHQL_ENDPOINT = "https://gmx-solana-sqd.squids.live/gmx-solana-base:prod/api/graphql"
SOLANA_RPC_ENDPOINT = "https://api.mainnet-beta.solana.com"
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPYg4GNiiAaA6VTxPBJW"
KAMINO_API_ENDPOINT = "https://api.kamino.finance"
KAMINO_RESOURCES_ENDPOINT = "https://cdn.kamino.com/resources.json"
KAMINO_SOLANA_RPC_ENDPOINT = (
    "https://helius-rpc.kamino.com/02996efe-bbc3-405f-8d87-845794261033"
)
KAMINO_VAULT_PROGRAM_ID = "KvauGMspG5k6rtzrqqn7WNn3oZdyKqLKwK2XWQ8FLjd"
KAMINO_FARMS_PROGRAM_ID = "FarmsPZpWu9i7Kky8tPN37rs2TpmMrAZrC7S7vJa91Hr"
GMTRADE_RPC_ENDPOINT = "https://rpc-1.gmtrade.xyz/"
GMTRADE_PRICE_TICKERS_ENDPOINT = (
    "https://gmtrade-web-backend.gmtrade.xyz/cache/prices/tickers"
)
GMTRADE_STORE_PROGRAM_ID = "Gmso1uvJnLbawvw7yezdfCDcPydwW2s2iqG3w6MDucLo"
GMTRADE_STORE_ADDRESS = "CTDLvGGXnoxvqLyTpGzdGLg9pD6JexKxKXSV8tqqo8bN"
GMTRADE_POSITION_DISCRIMINATOR = "VZMoMoKgZQb"
RETRYABLE_GRAPHQL_STATUS_CODES = {502, 503, 504}
OPTIONAL_POSITION_TIMEOUT = 8.0
OPTIONAL_LOOKUP_TIMEOUT = 6.0
GMTRADE_CSV_CACHE_MAX_SIZE = 256
KAMINO_CSV_CACHE_MAX_SIZE = 256
GMTRADE_MARKET_DECIMALS = 20
GMTRADE_PRICE_DECIMALS = 30
SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
DEFAULT_PUBLIC_KEY = "11111111111111111111111111111111"
SOLANA_PDA_MARKER = b"ProgramDerivedAddress"
SOLANA_ED25519_P = 2**255 - 19
SOLANA_ED25519_D = (
    -121665 * pow(121666, -1, SOLANA_ED25519_P)
) % SOLANA_ED25519_P
KAMINO_VAULT_STATE_DISCRIMINATOR = bytes([228, 196, 82, 165, 98, 210, 235, 152])
KAMINO_FARM_USER_STATE_DISCRIMINATOR = bytes(
    [72, 177, 85, 249, 76, 167, 186, 126]
)
KAMINO_VAULT_STATE_MIN_SIZE = 58728
KAMINO_FARM_USER_STATE_SIZE = 920
WAD_DECIMALS = 18
getcontext().prec = 50
GMTRADE_CSV_HEADER = [
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
GMTRADE_PERP_CSV_HEADER = [
    "position_address",
    "market",
    "side",
    "size_usd",
    "net_value_usd_estimated",
    "collateral_usd",
    "collateral_amount",
    "collateral_symbol",
    "entry_price_usd",
    "mark_price_usd",
    "pnl_usd_estimated",
    "leverage_estimated",
    "market_token_mint",
    "index_token_mint",
    "collateral_token_mint",
    "owner",
    "created_at",
    "increased_at",
    "decreased_at",
    "updated_at_slot",
    "trade_id",
    "size_in_tokens",
    "raw_size_usd",
    "raw_collateral_amount",
]
KAMINO_CSV_HEADER = [
    "type",
    "wallet",
    "vault_address",
    "share_mint",
    "vault_name",
    "share_symbol",
    "underlying_symbol",
    "share_balance",
    "underlying_amount",
    "token_price_usd",
    "value_usd",
    "apy",
    "apy_7d",
    "apy_30d",
    "apy_90d",
    "farm_rewards_apy",
    "actual_apy",
    "share_price",
    "tokens_per_share",
    "tokens_available",
    "tvl_tokens",
    "updated_at",
    "source_url",
]

router = APIRouter(prefix="/solana", tags=["solana"])
_gmtrade_csv_cache: dict[str, str] = {}
_gmtrade_perp_csv_cache: dict[str, str] = {}
_kamino_csv_cache: dict[str, str] = {}


async def _query_graphql(client: httpx.AsyncClient, query: str) -> dict[str, Any]:
    for attempt in range(3):
        try:
            response = await client.post(
                GRAPHQL_ENDPOINT,
                json={"query": query},
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError as exc:
            if attempt < 2:
                await asyncio.sleep(0.25 * (attempt + 1))
                continue
            raise HTTPException(
                status_code=502, detail=f"GMTrade GraphQL request failed: {exc}"
            ) from exc

        if (
            response.status_code in RETRYABLE_GRAPHQL_STATUS_CODES
            and attempt < 2
        ):
            await asyncio.sleep(0.25 * (attempt + 1))
            continue

        break

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


async def _rpc_request(client: httpx.AsyncClient, method: str, params: list[Any]) -> Any:
    try:
        response = await client.post(
            GMTRADE_RPC_ENDPOINT,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            headers={
                "Content-Type": "application/json",
                "Origin": "https://gmtrade.xyz",
            },
        )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502, detail=f"Solana RPC request failed: {exc}"
        ) from exc

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
        raise HTTPException(status_code=502, detail="Unexpected Solana RPC response")

    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message") or "Solana RPC returned an error"
        raise HTTPException(status_code=502, detail=message)

    return payload.get("result")


async def _solana_rpc_request(
    client: httpx.AsyncClient,
    method: str,
    params: list[Any],
    endpoint: str = SOLANA_RPC_ENDPOINT,
) -> Any:
    try:
        response = await client.post(
            endpoint,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            headers={"Content-Type": "application/json"},
        )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502, detail=f"Solana RPC request failed: {exc}"
        ) from exc

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
        raise HTTPException(status_code=502, detail="Unexpected Solana RPC response")

    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message") or "Solana RPC returned an error"
        raise HTTPException(status_code=502, detail=message)

    return payload.get("result")


def _base58_decode(value: str) -> bytes:
    number = 0
    for char in value:
        index = BASE58_ALPHABET.find(char)
        if index < 0:
            raise ValueError("invalid base58 character")
        number = number * 58 + index

    data = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    leading_zeroes = len(value) - len(value.lstrip(BASE58_ALPHABET[0]))
    return b"\x00" * leading_zeroes + data


def _base58_encode(data: bytes) -> str:
    number = int.from_bytes(data, "big")
    encoded = ""

    while number:
        number, remainder = divmod(number, 58)
        encoded = BASE58_ALPHABET[remainder] + encoded

    leading_zeroes = len(data) - len(data.lstrip(b"\x00"))
    return BASE58_ALPHABET[0] * leading_zeroes + (encoded or "")


def _is_ed25519_point_on_curve(data: bytes) -> bool:
    if len(data) != 32:
        return False

    sign = data[31] >> 7
    y = int.from_bytes(data, "little") & ((1 << 255) - 1)
    if y >= SOLANA_ED25519_P:
        return False

    y_squared = (y * y) % SOLANA_ED25519_P
    numerator = (y_squared - 1) % SOLANA_ED25519_P
    denominator = (SOLANA_ED25519_D * y_squared + 1) % SOLANA_ED25519_P
    try:
        x_squared = numerator * pow(denominator, -1, SOLANA_ED25519_P)
    except ValueError:
        return False
    x_squared %= SOLANA_ED25519_P

    if x_squared == 0:
        return sign == 0

    return pow(x_squared, (SOLANA_ED25519_P - 1) // 2, SOLANA_ED25519_P) == 1


def _find_program_address(seeds: list[bytes], program_id: str) -> str:
    program_id_bytes = _base58_decode(program_id)
    if len(program_id_bytes) != 32:
        raise ValueError("invalid Solana program id")
    for seed in seeds:
        if len(seed) > 32:
            raise ValueError("Solana PDA seed exceeds 32 bytes")

    for bump in range(255, -1, -1):
        digest = hashlib.sha256(
            b"".join([*seeds, bytes([bump]), program_id_bytes, SOLANA_PDA_MARKER])
        ).digest()
        if not _is_ed25519_point_on_curve(digest):
            return _base58_encode(digest)

    raise ValueError("unable to find a valid Solana program address")


def _derive_kamino_farm_user_state_address(farm_address: str, wallet: str) -> str:
    return _find_program_address(
        [b"user", _base58_decode(farm_address), _base58_decode(wallet)],
        KAMINO_FARMS_PROGRAM_ID,
    )


def _is_solana_address(value: str) -> bool:
    if not SOLANA_ADDRESS_RE.match(value):
        return False

    try:
        return len(_base58_decode(value)) == 32
    except ValueError:
        return False


def _read_u64_le(data: bytes, offset: int) -> int:
    return int.from_bytes(data[offset : offset + 8], "little", signed=False)


def _read_i64_le(data: bytes, offset: int) -> int:
    return int.from_bytes(data[offset : offset + 8], "little", signed=True)


def _read_u128_le(data: bytes, offset: int) -> int:
    return int.from_bytes(data[offset : offset + 16], "little", signed=False)


def _read_pubkey(data: bytes, offset: int) -> str:
    return _base58_encode(data[offset : offset + 32])


def _decode_rpc_account_data(value: Any) -> bytes:
    if isinstance(value, list) and value:
        value = value[0]
    if not isinstance(value, str):
        return b""

    try:
        return base64.b64decode(value)
    except Exception:
        return b""


def _unix_timestamp(value: int) -> str:
    if value <= 0:
        return ""
    return str(value)


def _decimal_scale(value: int | str | None, decimals: int) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        return Decimal(value) / (Decimal(10) ** decimals)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _format_decimal(value: Decimal | None, places: int = 10) -> str:
    if value is None or not value.is_finite():
        return ""

    quantized = value.quantize(Decimal(1).scaleb(-places)).normalize()
    if quantized == 0:
        return "0"
    return format(quantized, "f")


def _safe_decimal_divide(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _decode_gmtrade_perp_position(
    address: str, data: bytes
) -> dict[str, Any] | None:
    if len(data) < 296:
        return None

    size_in_usd = _read_u128_le(data, 216)
    if size_in_usd <= 0:
        return None

    kind = data[42]
    if kind not in {1, 2}:
        return None

    return {
        "position_address": address,
        "side": "long" if kind == 1 else "short",
        "store": _read_pubkey(data, 10),
        "owner": _read_pubkey(data, 56),
        "market_token_mint": _read_pubkey(data, 88),
        "collateral_token_mint": _read_pubkey(data, 120),
        "created_at": _unix_timestamp(_read_i64_le(data, 48)),
        "trade_id": _read_u64_le(data, 152),
        "increased_at": _unix_timestamp(_read_i64_le(data, 160)),
        "updated_at_slot": _read_u64_le(data, 168),
        "decreased_at": _unix_timestamp(_read_i64_le(data, 176)),
        "raw_size_in_tokens": _read_u128_le(data, 184),
        "raw_collateral_amount": _read_u128_le(data, 200),
        "raw_size_usd": size_in_usd,
        "raw_borrowing_factor": _read_u128_le(data, 232),
        "raw_funding_fee_amount_per_size": _read_u128_le(data, 248),
    }


async def _fetch_gmtrade_perp_positions(
    client: httpx.AsyncClient, wallet: str
) -> list[dict[str, Any]]:
    result = await _rpc_request(
        client,
        "getProgramAccounts",
        [
            GMTRADE_STORE_PROGRAM_ID,
            {
                "encoding": "base64",
                "commitment": "confirmed",
                "filters": [
                    {"memcmp": {"offset": 0, "bytes": GMTRADE_POSITION_DISCRIMINATOR}},
                    {"memcmp": {"offset": 10, "bytes": GMTRADE_STORE_ADDRESS}},
                    {"memcmp": {"offset": 56, "bytes": wallet}},
                ],
            },
        ],
    )

    accounts = result if isinstance(result, list) else []
    positions = []
    for account in accounts:
        if not isinstance(account, dict):
            continue
        address = account.get("pubkey")
        account_data = (account.get("account") or {}).get("data")
        if not isinstance(address, str):
            continue
        decoded = _decode_gmtrade_perp_position(
            address, _decode_rpc_account_data(account_data)
        )
        if decoded:
            positions.append(decoded)

    return positions


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


async def _fetch_token_decimals(
    client: httpx.AsyncClient, mints: list[str]
) -> dict[str, int]:
    if not mints:
        return {}

    decimals: dict[str, int] = {}
    for chunk in _chunked(mints, 100):
        result = await _rpc_request(
            client,
            "getMultipleAccounts",
            [chunk, {"encoding": "base64", "commitment": "confirmed"}],
        )
        values = result.get("value") if isinstance(result, dict) else []
        if not isinstance(values, list):
            continue

        for mint, item in zip(chunk, values, strict=False):
            if not isinstance(item, dict):
                continue
            data = _decode_rpc_account_data(item.get("data"))
            if len(data) > 44:
                decimals[mint] = data[44]

    return decimals


async def _fetch_gmtrade_price_tickers(
    client: httpx.AsyncClient,
) -> dict[str, dict[str, Any]]:
    try:
        response = await client.get(GMTRADE_PRICE_TICKERS_ENDPOINT)
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502, detail=f"GMTrade ticker request failed: {exc}"
        ) from exc

    if response.status_code < 200 or response.status_code >= 300:
        raise HTTPException(
            status_code=502,
            detail=f"GMTrade ticker request failed with status {response.status_code}",
        )

    try:
        payload = response.json()
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail="GMTrade ticker response is invalid JSON"
        ) from exc

    if not isinstance(payload, list):
        return {}

    result = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        address = item.get("tokenAddress")
        if isinstance(address, str) and address:
            result[address] = item
    return result


def _ticker_price_usd(
    tickers: dict[str, dict[str, Any]], token: str, decimals: int | None
) -> Decimal | None:
    ticker = tickers.get(token)
    if not ticker or decimals is None:
        return None

    min_price = ticker.get("minPrice")
    max_price = ticker.get("maxPrice")
    if min_price is None and max_price is None:
        return None

    prices = []
    for value in (min_price, max_price):
        if value is None:
            continue
        try:
            prices.append(Decimal(value))
        except (InvalidOperation, ValueError):
            continue

    if not prices:
        return None

    raw_price = sum(prices) / Decimal(len(prices))
    scale = GMTRADE_PRICE_DECIMALS - decimals
    if scale < 0:
        return None
    return raw_price / (Decimal(10) ** scale)


def _build_gmtrade_perp_rows(
    positions: list[dict[str, Any]],
    market_infos: dict[str, dict[str, Any]],
    token_decimals: dict[str, int],
    tickers: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for position in positions:
        market_token = position["market_token_mint"]
        collateral_token = position["collateral_token_mint"]
        market = market_infos.get(market_token, {})
        index_token = market.get("indexTokenMint") or ""
        index_decimals = token_decimals.get(index_token)
        collateral_decimals = token_decimals.get(collateral_token)
        size_usd = _decimal_scale(
            position["raw_size_usd"], GMTRADE_MARKET_DECIMALS
        )
        size_in_tokens = (
            _decimal_scale(position["raw_size_in_tokens"], index_decimals)
            if index_decimals is not None
            else Decimal(0)
        )
        collateral_amount = (
            _decimal_scale(position["raw_collateral_amount"], collateral_decimals)
            if collateral_decimals is not None
            else Decimal(0)
        )
        entry_price = _safe_decimal_divide(size_usd, size_in_tokens)
        mark_price = _ticker_price_usd(tickers, index_token, index_decimals)
        collateral_price = _ticker_price_usd(
            tickers, collateral_token, collateral_decimals
        )
        collateral_usd = (
            collateral_amount * collateral_price
            if collateral_price is not None
            else Decimal(0)
        )
        pnl = None
        if entry_price is not None and mark_price is not None:
            direction = Decimal(1) if position["side"] == "long" else Decimal(-1)
            pnl = (mark_price - entry_price) * size_in_tokens * direction
        net_value = collateral_usd + pnl if pnl is not None else None
        leverage = (
            _safe_decimal_divide(size_usd, net_value)
            if net_value is not None and net_value > 0
            else None
        )
        collateral_symbol = tickers.get(collateral_token, {}).get("tokenSymbol") or ""

        rows.append(
            {
                "position_address": position["position_address"],
                "market": market.get("name") or market_token,
                "side": position["side"],
                "size_usd": _format_decimal(size_usd, 6),
                "net_value_usd_estimated": _format_decimal(net_value, 6),
                "collateral_usd": _format_decimal(collateral_usd, 6),
                "collateral_amount": _format_decimal(collateral_amount, 10),
                "collateral_symbol": collateral_symbol,
                "entry_price_usd": _format_decimal(entry_price, 10),
                "mark_price_usd": _format_decimal(mark_price, 10),
                "pnl_usd_estimated": _format_decimal(pnl, 6),
                "leverage_estimated": _format_decimal(leverage, 6),
                "market_token_mint": market_token,
                "index_token_mint": index_token,
                "collateral_token_mint": collateral_token,
                "owner": position["owner"],
                "created_at": position["created_at"],
                "increased_at": position["increased_at"],
                "decreased_at": position["decreased_at"],
                "updated_at_slot": position["updated_at_slot"],
                "trade_id": position["trade_id"],
                "size_in_tokens": _format_decimal(size_in_tokens, 10),
                "raw_size_usd": position["raw_size_usd"],
                "raw_collateral_amount": position["raw_collateral_amount"],
            }
        )

    rows.sort(
        key=lambda row: Decimal(str(row["size_usd"] or "0")),
        reverse=True,
    )
    return rows


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


def _has_positive_balance(item: dict[str, Any]) -> bool:
    return _decimal_1e9(item.get("balance")) > 0


def _filter_positive_balance_items(
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [item for item in items if _has_positive_balance(item)]


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


async def _fetch_market_infos(
    client: httpx.AsyncClient, mints: list[str]
) -> dict[str, dict[str, Any]]:
    if not mints:
        return {}

    data = await _query_graphql(
        client,
        (
            f"{{ marketInfos(where:{{id_in:[{_quote_list(mints)}]}}) "
            "{ id name longTokenMint shortTokenMint indexTokenMint decimal } }"
        ),
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


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _decimal_or_zero(value: Any) -> Decimal:
    return _decimal_or_none(value) or Decimal(0)


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


async def _fetch_json(
    client: httpx.AsyncClient,
    url: str,
    detail_prefix: str,
    allow_404: bool = False,
) -> Any:
    try:
        response = await client.get(url)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"{detail_prefix}: {exc}") from exc

    if allow_404 and response.status_code == 404:
        return None

    if response.status_code < 200 or response.status_code >= 300:
        raise HTTPException(
            status_code=502,
            detail=f"{detail_prefix} failed with status {response.status_code}",
        )

    try:
        return response.json()
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"{detail_prefix} returned invalid JSON"
        ) from exc


async def _fetch_kamino_resources(client: httpx.AsyncClient) -> dict[str, Any]:
    payload = await _fetch_json(
        client, KAMINO_RESOURCES_ENDPOINT, "Kamino resources request"
    )
    return payload if isinstance(payload, dict) else {}


def _kamino_vault_resources(resources: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mainnet_resources = resources.get("mainnet-beta")
    if not isinstance(mainnet_resources, dict):
        return {}

    vaults = mainnet_resources.get("vaults")
    return vaults if isinstance(vaults, dict) else {}


def _normalize_kamino_vault_name(value: str | None) -> str:
    if not value:
        return ""

    stopwords = {"k", "kv", "vault", "kvault"}
    tokens = [
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if token not in stopwords
    ]
    return " ".join(sorted(tokens))


def _build_kamino_vault_resource_index(
    resources: dict[str, Any],
) -> dict[str, tuple[str, dict[str, Any]]]:
    index: dict[str, tuple[str, dict[str, Any]]] = {}
    for vault_address, vault in _kamino_vault_resources(resources).items():
        if not isinstance(vault_address, str) or not isinstance(vault, dict):
            continue

        name = vault.get("name") if isinstance(vault.get("name"), str) else ""
        token_symbol = (
            vault.get("tokenSymbol") if isinstance(vault.get("tokenSymbol"), str) else ""
        )
        candidates = [
            name,
            f"{token_symbol} {name}",
            f"{name} {token_symbol}",
            f"kVault {token_symbol} {name}",
            f"kVault {name}",
        ]

        for candidate in candidates:
            normalized = _normalize_kamino_vault_name(candidate)
            if normalized and normalized not in index:
                index[normalized] = (vault_address, vault)

    return index


def _match_kamino_vault_resource(
    metadata: dict[str, Any],
    resource_index: dict[str, tuple[str, dict[str, Any]]],
) -> tuple[str, dict[str, Any]] | None:
    for key in ("name", "symbol", "description"):
        value = metadata.get(key)
        if not isinstance(value, str):
            continue
        match = resource_index.get(_normalize_kamino_vault_name(value))
        if match:
            return match
    return None


async def _fetch_kamino_vault_token_metadata(
    client: httpx.AsyncClient, mint: str
) -> dict[str, Any] | None:
    payload = await _fetch_json(
        client,
        f"{KAMINO_API_ENDPOINT}/kvaults/mints/{mint}/metadata",
        "Kamino kVault metadata request",
        allow_404=True,
    )
    if not isinstance(payload, dict):
        return None

    name = payload.get("name")
    symbol = payload.get("symbol")
    is_kvault_name = isinstance(name, str) and "kvault" in name.lower()
    is_kvault_symbol = isinstance(symbol, str) and symbol.lower().startswith("kv")
    if is_kvault_name or is_kvault_symbol:
        return payload

    return None


async def _fetch_kamino_vault_metrics(
    client: httpx.AsyncClient, vault_address: str
) -> dict[str, Any]:
    payload = await _fetch_json(
        client,
        f"{KAMINO_API_ENDPOINT}/kvaults/{vault_address}/metrics",
        "Kamino kVault metrics request",
        allow_404=True,
    )
    return payload if isinstance(payload, dict) else {}


async def _fetch_multiple_account_infos(
    client: httpx.AsyncClient,
    addresses: list[str],
    endpoint: str = KAMINO_SOLANA_RPC_ENDPOINT,
) -> dict[str, dict[str, Any] | None]:
    if not addresses:
        return {}

    account_infos: dict[str, dict[str, Any] | None] = {}
    for index in range(0, len(addresses), 100):
        chunk = addresses[index : index + 100]
        result = await _solana_rpc_request(
            client,
            "getMultipleAccounts",
            [chunk, {"encoding": "base64", "commitment": "confirmed"}],
            endpoint=endpoint,
        )
        values = result.get("value") if isinstance(result, dict) else []
        if not isinstance(values, list):
            values = []
        for address, value in zip(chunk, values, strict=False):
            account_infos[address] = value if isinstance(value, dict) else None

    return account_infos


def _rpc_account_info_data(account_info: dict[str, Any] | None) -> bytes:
    if not isinstance(account_info, dict):
        return b""
    return _decode_rpc_account_data(account_info.get("data"))


def _decode_null_padded_ascii(data: bytes) -> str:
    return data.split(b"\x00", 1)[0].decode("utf-8", errors="ignore")


def _parse_kamino_vault_state(
    vault_address: str, account_info: dict[str, Any] | None
) -> dict[str, Any] | None:
    if not isinstance(account_info, dict):
        return None
    if account_info.get("owner") != KAMINO_VAULT_PROGRAM_ID:
        return None

    data = _rpc_account_info_data(account_info)
    if (
        len(data) < KAMINO_VAULT_STATE_MIN_SIZE
        or data[:8] != KAMINO_VAULT_STATE_DISCRIMINATOR
    ):
        return None

    return {
        "vault_address": vault_address,
        "token_mint": _read_pubkey(data, 80),
        "token_mint_decimals": _read_u64_le(data, 112),
        "shares_mint": _read_pubkey(data, 184),
        "shares_mint_decimals": _read_u64_le(data, 216),
        "token_available": _read_u64_le(data, 224),
        "shares_issued": _read_u64_le(data, 232),
        "name": _decode_null_padded_ascii(data[58528:58568]),
        "vault_farm": _read_pubkey(data, 58600),
        "first_loss_capital_farm": _read_pubkey(data, 58696),
    }


async def _fetch_kamino_vault_states(
    client: httpx.AsyncClient, vault_addresses: list[str]
) -> dict[str, dict[str, Any]]:
    account_infos = await _fetch_multiple_account_infos(client, vault_addresses)
    states: dict[str, dict[str, Any]] = {}
    for vault_address in vault_addresses:
        state = _parse_kamino_vault_state(
            vault_address, account_infos.get(vault_address)
        )
        if state is not None:
            states[vault_address] = state
    return states


def _parse_kamino_farm_staked_shares(
    account_info: dict[str, Any] | None, share_decimals: int
) -> Decimal:
    if not isinstance(account_info, dict):
        return Decimal(0)
    if account_info.get("owner") != KAMINO_FARMS_PROGRAM_ID:
        return Decimal(0)

    data = _rpc_account_info_data(account_info)
    if (
        len(data) != KAMINO_FARM_USER_STATE_SIZE
        or data[:8] != KAMINO_FARM_USER_STATE_DISCRIMINATOR
    ):
        return Decimal(0)

    active_stake_scaled = _read_u128_le(data, 408)
    active_stake_lamports = Decimal(active_stake_scaled) / (
        Decimal(10) ** WAD_DECIMALS
    )
    if active_stake_lamports < 1:
        return Decimal(0)
    return active_stake_lamports / (Decimal(10) ** share_decimals)


async def _fetch_kamino_staked_share_balances(
    client: httpx.AsyncClient,
    wallet: str,
    vault_states: dict[str, dict[str, Any]],
) -> dict[str, Decimal]:
    pda_entries: dict[str, tuple[str, int]] = {}
    for vault_address, state in vault_states.items():
        for farm_key in ("vault_farm", "first_loss_capital_farm"):
            farm_address = state.get(farm_key)
            if not isinstance(farm_address, str) or farm_address == DEFAULT_PUBLIC_KEY:
                continue
            try:
                user_state_address = _derive_kamino_farm_user_state_address(
                    farm_address, wallet
                )
            except ValueError:
                continue
            share_decimals = int(state.get("shares_mint_decimals") or 0)
            pda_entries[user_state_address] = (vault_address, share_decimals)

    account_infos = await _fetch_multiple_account_infos(client, list(pda_entries))
    balances: dict[str, Decimal] = {}
    for user_state_address, (vault_address, share_decimals) in pda_entries.items():
        staked_shares = _parse_kamino_farm_staked_shares(
            account_infos.get(user_state_address), share_decimals
        )
        if staked_shares > 0:
            balances[vault_address] = (
                balances.get(vault_address, Decimal(0)) + staked_shares
            )

    return balances


async def _fetch_token_accounts_by_owner(
    client: httpx.AsyncClient, wallet: str, token_program_id: str
) -> list[dict[str, Any]]:
    result = await _solana_rpc_request(
        client,
        "getTokenAccountsByOwner",
        [
            wallet,
            {"programId": token_program_id},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
        endpoint=KAMINO_SOLANA_RPC_ENDPOINT,
    )
    values = result.get("value") if isinstance(result, dict) else []
    return values if isinstance(values, list) else []


def _is_unrecognized_token_program_error(exc: HTTPException) -> bool:
    detail = str(exc.detail)
    return exc.status_code == 502 and "unrecognized Token program id" in detail


async def _fetch_token_accounts(
    client: httpx.AsyncClient, wallet: str
) -> list[dict[str, Any]]:
    token_accounts_result, token_2022_accounts_result = await asyncio.gather(
        _fetch_token_accounts_by_owner(client, wallet, SPL_TOKEN_PROGRAM_ID),
        _fetch_token_accounts_by_owner(client, wallet, SPL_TOKEN_2022_PROGRAM_ID),
        return_exceptions=True,
    )

    if isinstance(token_accounts_result, Exception):
        raise token_accounts_result

    if isinstance(token_2022_accounts_result, HTTPException):
        if _is_unrecognized_token_program_error(token_2022_accounts_result):
            token_2022_accounts_result = []
        else:
            raise token_2022_accounts_result
    elif isinstance(token_2022_accounts_result, Exception):
        raise token_2022_accounts_result

    token_accounts = token_accounts_result
    token_2022_accounts = token_2022_accounts_result
    return [*token_accounts, *token_2022_accounts]


def _parse_token_account_position(item: dict[str, Any]) -> dict[str, Any] | None:
    account = item.get("account")
    if not isinstance(account, dict):
        return None

    data = account.get("data")
    if not isinstance(data, dict):
        return None

    parsed = data.get("parsed")
    if not isinstance(parsed, dict):
        return None

    info = parsed.get("info")
    if not isinstance(info, dict):
        return None

    token_amount = info.get("tokenAmount")
    if not isinstance(token_amount, dict):
        return None

    mint = info.get("mint")
    if not isinstance(mint, str) or not mint:
        return None

    balance = _decimal_or_none(token_amount.get("uiAmountString"))
    if balance is None:
        raw_amount = _decimal_or_none(token_amount.get("amount"))
        decimals = token_amount.get("decimals")
        if raw_amount is not None and isinstance(decimals, int):
            balance = raw_amount / (Decimal(10) ** decimals)

    if balance is None or balance <= 0:
        return None

    token_account = item.get("pubkey")
    return {
        "token_account": token_account if isinstance(token_account, str) else "",
        "mint": mint,
        "balance": balance,
    }


def _build_kamino_vault_token_positions(
    token_positions: list[dict[str, Any]],
    vault_states: dict[str, dict[str, Any]],
    staked_share_balances: dict[str, Decimal],
) -> list[dict[str, Any]]:
    unstaked_balances_by_mint: dict[str, Decimal] = {}
    for token_position in token_positions:
        mint = token_position["mint"]
        unstaked_balances_by_mint[mint] = (
            unstaked_balances_by_mint.get(mint, Decimal(0))
            + token_position["balance"]
        )

    positions = []
    for vault_address, state in vault_states.items():
        share_mint = state.get("shares_mint")
        if not isinstance(share_mint, str) or not share_mint:
            continue

        unstaked_balance = unstaked_balances_by_mint.get(share_mint, Decimal(0))
        staked_balance = staked_share_balances.get(vault_address, Decimal(0))
        total_balance = unstaked_balance + staked_balance
        if total_balance <= 0:
            continue

        positions.append(
            {
                "mint": share_mint,
                "balance": total_balance,
                "unstaked_balance": unstaked_balance,
                "staked_balance": staked_balance,
                "vault_address": vault_address,
            }
        )

    return positions


def _build_kamino_rows(
    wallet: str,
    token_positions: list[dict[str, Any]],
    metadata_by_mint: dict[str, dict[str, Any]],
    resources: dict[str, Any],
    metrics_by_vault: dict[str, dict[str, Any]],
    updated_at: str,
) -> list[dict[str, Any]]:
    resource_index = _build_kamino_vault_resource_index(resources)
    resources_by_vault = _kamino_vault_resources(resources)
    rows = []

    for token_position in token_positions:
        mint = token_position["mint"]
        metadata = metadata_by_mint.get(mint, {})
        vault_address = token_position.get("vault_address")
        resource = (
            resources_by_vault.get(vault_address)
            if isinstance(vault_address, str)
            else None
        )
        if not isinstance(resource, dict):
            resource_match = _match_kamino_vault_resource(metadata, resource_index)
            vault_address = resource_match[0] if resource_match else ""
            resource = resource_match[1] if resource_match else {}

        if not metadata and not vault_address:
            continue

        metrics = metrics_by_vault.get(vault_address, {})
        balance = token_position["balance"]
        tokens_per_share = _decimal_or_none(metrics.get("tokensPerShare"))
        if tokens_per_share is None:
            tokens_per_share = _decimal_or_none(metrics.get("sharePrice"))
        token_price_usd = _decimal_or_none(metrics.get("tokenPrice"))
        underlying_amount = (
            balance * tokens_per_share if tokens_per_share is not None else None
        )
        value_usd = (
            underlying_amount * token_price_usd
            if underlying_amount is not None and token_price_usd is not None
            else None
        )
        source_url = (
            f"{KAMINO_API_ENDPOINT}/kvaults/{vault_address}/metrics"
            if vault_address
            else f"{KAMINO_API_ENDPOINT}/kvaults/mints/{mint}/metadata"
        )
        token_symbol = (
            resource.get("tokenSymbol")
            if isinstance(resource.get("tokenSymbol"), str)
            else ""
        )

        rows.append(
            {
                "type": "kVault",
                "wallet": wallet,
                "vault_address": vault_address,
                "share_mint": mint,
                "vault_name": resource.get("name") or metadata.get("name") or "",
                "share_symbol": metadata.get("symbol") or "",
                "underlying_symbol": token_symbol,
                "share_balance": _format_decimal(balance, 12),
                "underlying_amount": _format_decimal(underlying_amount, 12),
                "token_price_usd": _format_decimal(token_price_usd, 12),
                "value_usd": _format_decimal(value_usd, 6),
                "apy": _format_decimal(_decimal_or_none(metrics.get("apy")), 12),
                "apy_7d": _format_decimal(_decimal_or_none(metrics.get("apy7d")), 12),
                "apy_30d": _format_decimal(_decimal_or_none(metrics.get("apy30d")), 12),
                "apy_90d": _format_decimal(_decimal_or_none(metrics.get("apy90d")), 12),
                "farm_rewards_apy": _format_decimal(
                    _decimal_or_none(metrics.get("apyFarmRewards")), 12
                ),
                "actual_apy": _format_decimal(
                    _decimal_or_none(metrics.get("apyActual")), 12
                ),
                "share_price": _format_decimal(
                    _decimal_or_none(metrics.get("sharePrice")), 12
                ),
                "tokens_per_share": _format_decimal(tokens_per_share, 12),
                "tokens_available": _format_decimal(
                    _decimal_or_none(metrics.get("tokensAvailable")), 6
                ),
                "tvl_tokens": _format_decimal(
                    _decimal_or_none(metrics.get("tokensInvested")), 6
                ),
                "updated_at": updated_at,
                "source_url": source_url,
            }
        )

    rows.sort(key=lambda row: _decimal_or_zero(row["value_usd"]), reverse=True)
    return rows


async def _fetch_optional_lookup(fetcher: Any, *args: Any) -> dict[str, Any]:
    try:
        return await asyncio.wait_for(fetcher(*args), timeout=OPTIONAL_LOOKUP_TIMEOUT)
    except TimeoutError:
        return {}
    except httpx.HTTPError:
        return {}
    except HTTPException as exc:
        if exc.status_code == 502:
            return {}
        raise


async def _fetch_optional_positions(fetcher: Any, *args: Any) -> list[dict[str, Any]]:
    try:
        items = await asyncio.wait_for(
            fetcher(*args), timeout=OPTIONAL_POSITION_TIMEOUT
        )
    except TimeoutError:
        return []
    except httpx.HTTPError:
        return []
    except HTTPException as exc:
        if exc.status_code == 502:
            return []
        raise

    return items if isinstance(items, list) else []


async def _fetch_required_positions(fetcher: Any, *args: Any) -> list[dict[str, Any]]:
    try:
        items = await asyncio.wait_for(
            fetcher(*args), timeout=OPTIONAL_POSITION_TIMEOUT
        )
    except TimeoutError as exc:
        raise HTTPException(
            status_code=502, detail="GMTrade positions timed out"
        ) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502, detail=f"GMTrade positions request failed: {exc}"
        ) from exc

    return items if isinstance(items, list) else []


async def _fetch_required_lookup(fetcher: Any, *args: Any) -> dict[str, Any]:
    try:
        items = await asyncio.wait_for(fetcher(*args), timeout=OPTIONAL_LOOKUP_TIMEOUT)
    except TimeoutError as exc:
        raise HTTPException(
            status_code=502, detail="GMTrade lookup timed out"
        ) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502, detail=f"GMTrade lookup request failed: {exc}"
        ) from exc

    return items if isinstance(items, dict) else {}


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


def _get_cached_gmtrade_csv(wallet: str) -> str | None:
    cached = _gmtrade_csv_cache.get(wallet)
    if cached is not None:
        _gmtrade_csv_cache[wallet] = _gmtrade_csv_cache.pop(wallet)
    return cached


def _set_cached_gmtrade_csv(wallet: str, content: str) -> None:
    if wallet in _gmtrade_csv_cache:
        _gmtrade_csv_cache.pop(wallet)
    elif len(_gmtrade_csv_cache) >= GMTRADE_CSV_CACHE_MAX_SIZE:
        _gmtrade_csv_cache.pop(next(iter(_gmtrade_csv_cache)))

    _gmtrade_csv_cache[wallet] = content


def _get_cached_gmtrade_perp_csv(wallet: str) -> str | None:
    cached = _gmtrade_perp_csv_cache.get(wallet)
    if cached is not None:
        _gmtrade_perp_csv_cache[wallet] = _gmtrade_perp_csv_cache.pop(wallet)
    return cached


def _set_cached_gmtrade_perp_csv(wallet: str, content: str) -> None:
    if wallet in _gmtrade_perp_csv_cache:
        _gmtrade_perp_csv_cache.pop(wallet)
    elif len(_gmtrade_perp_csv_cache) >= GMTRADE_CSV_CACHE_MAX_SIZE:
        _gmtrade_perp_csv_cache.pop(next(iter(_gmtrade_perp_csv_cache)))

    _gmtrade_perp_csv_cache[wallet] = content


def _get_cached_kamino_csv(wallet: str) -> str | None:
    cached = _kamino_csv_cache.get(wallet)
    if cached is not None:
        _kamino_csv_cache[wallet] = _kamino_csv_cache.pop(wallet)
    return cached


def _set_cached_kamino_csv(wallet: str, content: str) -> None:
    if wallet in _kamino_csv_cache:
        _kamino_csv_cache.pop(wallet)
    elif len(_kamino_csv_cache) >= KAMINO_CSV_CACHE_MAX_SIZE:
        _kamino_csv_cache.pop(next(iter(_kamino_csv_cache)))

    _kamino_csv_cache[wallet] = content


def _render_gmtrade_csv(rows: list[dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(GMTRADE_CSV_HEADER)

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

    return output.getvalue()


def _render_gmtrade_perp_csv(rows: list[dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(GMTRADE_PERP_CSV_HEADER)

    for row in rows:
        writer.writerow([row.get(header, "") for header in GMTRADE_PERP_CSV_HEADER])

    return output.getvalue()


def _render_kamino_csv(rows: list[dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(KAMINO_CSV_HEADER)

    for row in rows:
        writer.writerow([row.get(header, "") for header in KAMINO_CSV_HEADER])

    return output.getvalue()


async def _build_gmtrade_csv_content(normalized_wallet: str) -> str:
    async with httpx.AsyncClient(timeout=20.0) as client:
        gm_users, glv_users = await asyncio.gather(
            _fetch_required_positions(
                _fetch_market_gm_users, client, normalized_wallet
            ),
            _fetch_required_positions(_fetch_glv_users, client, normalized_wallet),
        )
        gm_users = _filter_positive_balance_items(gm_users)
        glv_users = _filter_positive_balance_items(glv_users)
        gm_mints = _collect_unique_mints(gm_users, "marketToken")
        glv_mints = _collect_unique_mints(glv_users, "glvToken")
        market_infos, gm_infos, glv_infos, asset_names = await asyncio.gather(
            _fetch_required_lookup(_fetch_market_infos, client, gm_mints),
            _fetch_required_lookup(_fetch_market_gm_infos, client, gm_mints),
            _fetch_required_lookup(_fetch_glv_infos, client, glv_mints),
            _fetch_required_lookup(_fetch_asset_names, client, glv_mints),
        )

    rows = _build_gm_rows(gm_users, market_infos, gm_infos) + _build_glv_rows(
        glv_users, glv_infos, asset_names
    )
    rows.sort(key=lambda item: item["value_usd"], reverse=True)
    return _render_gmtrade_csv(rows)


async def _build_gmtrade_perp_csv_content(normalized_wallet: str) -> str:
    async with httpx.AsyncClient(timeout=20.0) as client:
        positions = await _fetch_gmtrade_perp_positions(client, normalized_wallet)
        market_mints = _collect_unique_mints(positions, "market_token_mint")
        market_infos = await _fetch_required_lookup(
            _fetch_market_infos, client, market_mints
        )
        token_mints = _collect_unique_mints(
            [
                *positions,
                *[
                    {"token": info.get("indexTokenMint", "")}
                    for info in market_infos.values()
                ],
            ],
            "token",
        )
        token_mints.extend(
            mint
            for mint in _collect_unique_mints(positions, "collateral_token_mint")
            if mint not in token_mints
        )
        token_decimals, tickers = await asyncio.gather(
            _fetch_required_lookup(_fetch_token_decimals, client, token_mints),
            _fetch_required_lookup(_fetch_gmtrade_price_tickers, client),
        )

    rows = _build_gmtrade_perp_rows(
        positions, market_infos, token_decimals, tickers
    )
    return _render_gmtrade_perp_csv(rows)


async def _build_kamino_csv_content(normalized_wallet: str) -> str:
    async with httpx.AsyncClient(timeout=20.0) as client:
        resources, token_accounts = await asyncio.gather(
            _fetch_kamino_resources(client),
            _fetch_token_accounts(client, normalized_wallet),
        )
        resource_vault_addresses = [
            vault_address
            for vault_address in _kamino_vault_resources(resources)
            if isinstance(vault_address, str)
        ]
        vault_states = await _fetch_kamino_vault_states(
            client, resource_vault_addresses
        )
        staked_share_balances = await _fetch_kamino_staked_share_balances(
            client, normalized_wallet, vault_states
        )
        token_positions = [
            parsed
            for parsed in (
                _parse_token_account_position(account) for account in token_accounts
            )
            if parsed is not None
        ]
        kamino_token_positions = _build_kamino_vault_token_positions(
            token_positions, vault_states, staked_share_balances
        )
        known_share_mints = {
            state["shares_mint"]
            for state in vault_states.values()
            if isinstance(state.get("shares_mint"), str)
        }
        fallback_token_positions = [
            token_position
            for token_position in token_positions
            if token_position["mint"] not in known_share_mints
        ]

        unique_mints = _collect_unique_mints(
            [*kamino_token_positions, *fallback_token_positions], "mint"
        )
        metadata_results = await asyncio.gather(
            *(
                _fetch_kamino_vault_token_metadata(client, mint)
                for mint in unique_mints
            )
        )
        metadata_by_mint = {
            mint: metadata
            for mint, metadata in zip(unique_mints, metadata_results, strict=False)
            if metadata is not None
        }
        resource_index = _build_kamino_vault_resource_index(resources)
        vault_addresses = []
        matched_fallback_positions = []
        for token_position in fallback_token_positions:
            metadata = metadata_by_mint.get(token_position["mint"])
            if not metadata:
                continue
            resource_match = _match_kamino_vault_resource(metadata, resource_index)
            if not resource_match:
                continue
            matched_fallback_positions.append(token_position)

        final_token_positions = [*kamino_token_positions, *matched_fallback_positions]
        for token_position in final_token_positions:
            vault_address = token_position.get("vault_address")
            if not isinstance(vault_address, str) or not vault_address:
                metadata = metadata_by_mint.get(token_position["mint"], {})
                resource_match = _match_kamino_vault_resource(metadata, resource_index)
                vault_address = resource_match[0] if resource_match else ""
            if vault_address and vault_address not in vault_addresses:
                vault_addresses.append(vault_address)

        metrics_results = await asyncio.gather(
            *(
                _fetch_kamino_vault_metrics(client, vault_address)
                for vault_address in vault_addresses
            )
        )
        metrics_by_vault = dict(zip(vault_addresses, metrics_results, strict=False))

    rows = _build_kamino_rows(
        normalized_wallet,
        final_token_positions,
        metadata_by_mint,
        resources,
        metrics_by_vault,
        _now_iso(),
    )
    return _render_kamino_csv(rows)


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

    try:
        content = await _build_gmtrade_csv_content(normalized_wallet)
    except HTTPException as exc:
        if exc.status_code < 500:
            raise

        content = _get_cached_gmtrade_csv(normalized_wallet)
        if content is None:
            content = _render_gmtrade_csv([])
    else:
        _set_cached_gmtrade_csv(normalized_wallet, content)

    return Response(content=content, media_type="text/csv")


@router.get(
    "/kamino.csv",
    summary="Export Solana Kamino kVault positions for Google Sheets",
    description=(
        "Returns a CSV table with positive Kamino kVault share-token positions "
        "for a Solana wallet address. Suitable for Google Sheets IMPORTDATA."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_kamino_csv(
    wallet: str = Query(..., description="Solana wallet address"),
):
    normalized_wallet = wallet.strip()
    if not normalized_wallet:
        raise HTTPException(status_code=400, detail="wallet is required")
    if not _is_solana_address(normalized_wallet):
        raise HTTPException(status_code=400, detail="invalid Solana wallet address")

    try:
        content = await _build_kamino_csv_content(normalized_wallet)
    except HTTPException as exc:
        if exc.status_code < 500:
            raise

        content = _get_cached_kamino_csv(normalized_wallet)
        if content is None:
            content = _render_kamino_csv([])
    else:
        _set_cached_kamino_csv(normalized_wallet, content)

    return Response(content=content, media_type="text/csv")


@router.get(
    "/gmtrade-perps.csv",
    summary="Export Solana GMTrade perp positions for Google Sheets",
    description=(
        "Returns a CSV table with open GMTrade perp positions for a Solana wallet "
        "address. Suitable for Google Sheets IMPORTDATA."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_gmtrade_perps_csv(
    wallet: str = Query(..., description="Solana wallet address"),
):
    normalized_wallet = wallet.strip()
    if not normalized_wallet:
        raise HTTPException(status_code=400, detail="wallet is required")
    if not _is_solana_address(normalized_wallet):
        raise HTTPException(status_code=400, detail="invalid Solana wallet address")

    try:
        content = await _build_gmtrade_perp_csv_content(normalized_wallet)
    except HTTPException as exc:
        if exc.status_code < 500:
            raise

        content = _get_cached_gmtrade_perp_csv(normalized_wallet)
        if content is None:
            content = _render_gmtrade_perp_csv([])
    else:
        _set_cached_gmtrade_perp_csv(normalized_wallet, content)

    return Response(content=content, media_type="text/csv")
