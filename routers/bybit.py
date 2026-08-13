import csv
import hashlib
import hmac
import io
import json
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, SecretStr

from bybit_capsule import (
    BYBIT_CAPSULE_MAX_LENGTH,
    decrypt_bybit_credentials,
    encrypt_bybit_credentials,
)
from outbound_queue import queued_async_client


BYBIT_API_REGIONS = {
    "global": "https://api.bybit.com",
    "indonesia": "https://api.bybit.id",
    "eu": "https://api.bybit.eu",
    "netherlands": "https://api.bybit.nl",
    "turkey": "https://api.bybit.tr",
    "kazakhstan": "https://api.bybit.kz",
    "georgia": "https://api.bybitgeorgia.ge",
    "uae": "https://api.bybit.ae",
    "japan": "https://api.manepa.jp",
}
BYBIT_RECV_WINDOW = "5000"
BYBIT_MAX_PAGES = 20
BYBIT_CSV_HEADER = [
    "row_type",
    "id",
    "account_type",
    "category",
    "symbol",
    "coin",
    "side",
    "position_idx",
    "total_equity_usd",
    "total_wallet_balance_usd",
    "total_margin_balance_usd",
    "total_available_balance_usd",
    "total_perp_upl_usd",
    "total_initial_margin_usd",
    "total_maintenance_margin_usd",
    "account_im_rate",
    "account_mm_rate",
    "equity",
    "usd_value",
    "wallet_balance",
    "locked",
    "borrow_amount",
    "spot_borrow",
    "unrealised_pnl",
    "cum_realised_pnl",
    "margin_collateral",
    "collateral_switch",
    "size",
    "position_value",
    "avg_price",
    "mark_price",
    "break_even_price",
    "liquidation_price",
    "leverage",
    "position_im",
    "position_mm",
    "take_profit",
    "stop_loss",
    "trailing_stop",
    "position_status",
    "is_reduce_only",
    "created_at",
    "updated_at",
]

router = APIRouter(prefix="/bybit", tags=["bybit"])


class BybitCapsuleRequest(BaseModel):
    api_key: str
    api_secret: SecretStr
    region: str = "global"


def _api_base_url(region: str) -> str:
    base_url = BYBIT_API_REGIONS.get(region.strip().lower())
    if base_url is None:
        supported = ", ".join(BYBIT_API_REGIONS)
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported Bybit API region. Supported: {supported}",
        )
    return base_url


def _signed_headers(
    api_key: str,
    api_secret: str,
    query_string: str,
    timestamp_ms: int | None = None,
) -> dict[str, str]:
    timestamp = str(timestamp_ms or int(time.time() * 1000))
    payload = f"{timestamp}{api_key}{BYBIT_RECV_WINDOW}{query_string}"
    signature = hmac.new(
        api_secret.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    return {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": BYBIT_RECV_WINDOW,
        "X-BAPI-SIGN": signature,
        "Accept": "application/json",
    }


async def _get_bybit_json(
    client: httpx.AsyncClient,
    base_url: str,
    path: str,
    api_key: str,
    api_secret: str,
    params: dict[str, object] | None = None,
) -> dict[str, Any]:
    query_string = urlencode(
        [(key, str(value)) for key, value in (params or {}).items()]
    )
    response = await client.get(
        f"{base_url}{path}",
        params=query_string,
        headers=_signed_headers(api_key, api_secret, query_string),
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Bybit returned invalid JSON") from exc

    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Bybit API returned HTTP {response.status_code}",
        )
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="Invalid Bybit API response")

    ret_code = payload.get("retCode")
    if ret_code != 0:
        message = str(payload.get("retMsg") or "Unknown Bybit API error")
        status_code = 401 if ret_code in {10003, 10004, 10005, 33004} else 502
        raise HTTPException(status_code=status_code, detail=f"Bybit API error: {message}")
    return payload


async def _get_bybit_public_json(
    client: httpx.AsyncClient,
    base_url: str,
    path: str,
    params: dict[str, object] | None = None,
) -> dict[str, Any]:
    response = await client.get(
        f"{base_url}{path}",
        params=params,
        headers={"Accept": "application/json"},
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Bybit returned invalid JSON") from exc

    if response.status_code >= 400 or not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="Invalid Bybit API response")
    if payload.get("retCode") != 0:
        message = str(payload.get("retMsg") or "Unknown Bybit API error")
        raise HTTPException(status_code=502, detail=f"Bybit API error: {message}")
    return payload


async def _validate_view_only_credentials(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    api_secret: str,
) -> dict[str, object]:
    payload = await _get_bybit_json(
        client,
        base_url,
        "/v5/user/query-api",
        api_key,
        api_secret,
    )
    result = payload.get("result")
    if not isinstance(result, dict) or result.get("readOnly") != 1:
        raise HTTPException(
            status_code=400,
            detail="Bybit API key must be Read-only",
        )
    return {
        "read_only": True,
        "note": str(result.get("note") or ""),
    }


async def _fetch_wallet_balance(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    api_secret: str,
) -> dict[str, Any]:
    payload = await _get_bybit_json(
        client,
        base_url,
        "/v5/account/wallet-balance",
        api_key,
        api_secret,
        {"accountType": "UNIFIED"},
    )
    result = payload.get("result")
    accounts = result.get("list") if isinstance(result, dict) else None
    if not isinstance(accounts, list) or not accounts or not isinstance(accounts[0], dict):
        raise HTTPException(status_code=502, detail="Invalid Bybit wallet response")
    return accounts[0]


async def _fetch_funding_balances(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    api_secret: str,
) -> list[dict[str, Any]]:
    payload = await _get_bybit_json(
        client,
        base_url,
        "/v5/asset/transfer/query-account-coins-balance",
        api_key,
        api_secret,
        {"accountType": "FUND"},
    )
    result = payload.get("result")
    balances = result.get("balance") if isinstance(result, dict) else None
    if not isinstance(balances, list) or not all(
        isinstance(item, dict) for item in balances
    ):
        raise HTTPException(status_code=502, detail="Invalid Bybit Funding response")
    return balances


async def _fetch_spot_usd_prices(
    client: httpx.AsyncClient,
    base_url: str,
) -> dict[str, Decimal]:
    payload = await _get_bybit_public_json(
        client,
        base_url,
        "/v5/market/tickers",
        {"category": "spot"},
    )
    result = payload.get("result")
    tickers = result.get("list") if isinstance(result, dict) else None
    if not isinstance(tickers, list):
        raise HTTPException(status_code=502, detail="Invalid Bybit ticker response")

    prices: dict[str, Decimal] = {}
    for ticker in tickers:
        if not isinstance(ticker, dict):
            continue
        symbol = _string(ticker.get("symbol")).upper()
        quote = next(
            (candidate for candidate in ("USDT", "USDC") if symbol.endswith(candidate)),
            "",
        )
        coin = symbol[: -len(quote)] if quote else ""
        price = _decimal(ticker.get("usdIndexPrice")) or _decimal(
            ticker.get("lastPrice")
        )
        if coin and price > 0:
            prices.setdefault(coin, price)
    return prices


async def _fetch_position_category(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    api_secret: str,
    category: str,
    settle_coin: str | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor = ""
    for _ in range(BYBIT_MAX_PAGES):
        params: dict[str, object] = {"category": category, "limit": 200}
        if settle_coin:
            params["settleCoin"] = settle_coin
        if cursor:
            params["cursor"] = cursor
        payload = await _get_bybit_json(
            client,
            base_url,
            "/v5/position/list",
            api_key,
            api_secret,
            params,
        )
        result = payload.get("result")
        positions = result.get("list") if isinstance(result, dict) else None
        if not isinstance(positions, list):
            raise HTTPException(status_code=502, detail="Invalid Bybit positions response")
        rows.extend(item for item in positions if isinstance(item, dict))
        cursor = str(result.get("nextPageCursor") or "")
        if not cursor:
            return rows
    raise HTTPException(status_code=502, detail="Bybit position pagination limit exceeded")


def _string(value: object | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _decimal(value: object | None) -> Decimal:
    try:
        return Decimal(str(value)) if value not in (None, "") else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _timestamp(value: object | None) -> str:
    milliseconds = int(_decimal(value))
    if milliseconds <= 0:
        return ""
    try:
        return datetime.fromtimestamp(milliseconds / 1000, tz=UTC).isoformat()
    except (OverflowError, OSError, ValueError):
        return ""


def _empty_row() -> dict[str, str]:
    return {column: "" for column in BYBIT_CSV_HEADER}


def _funding_usd_value(
    coin: str,
    balance: Decimal,
    prices: dict[str, Decimal],
) -> Decimal | None:
    if coin in {"USD", "USDC", "USDT"}:
        return balance
    price = prices.get(coin)
    return balance * price if price is not None else None


def _render_bybit_csv(
    account: dict[str, Any],
    positions: list[tuple[str, dict[str, Any]]],
    funding_balances: list[dict[str, Any]] | None = None,
    spot_usd_prices: dict[str, Decimal] | None = None,
) -> str:
    rows: list[dict[str, str]] = []
    funding_rows: list[dict[str, str]] = []
    funding_total_usd = Decimal(0)

    if funding_balances is not None:
        prices = spot_usd_prices or {}
        for item in funding_balances:
            coin = _string(item.get("coin")).upper()
            wallet_balance = _decimal(item.get("walletBalance"))
            if not coin or wallet_balance == 0:
                continue
            usd_value = _funding_usd_value(coin, wallet_balance, prices)
            if usd_value is not None:
                funding_total_usd += usd_value
            row = _empty_row()
            row.update(
                {
                    "row_type": "funding_balance",
                    "id": f"bybit:funding:balance:{coin}",
                    "account_type": "FUND",
                    "coin": coin,
                    "equity": _string(item.get("walletBalance")),
                    "usd_value": _string(usd_value),
                    "wallet_balance": _string(item.get("walletBalance")),
                }
            )
            funding_rows.append(row)

        total = _empty_row()
        total.update(
            {
                "row_type": "account_summary",
                "id": "bybit:total",
                "account_type": "ALL_WALLETS",
                "total_equity_usd": _string(
                    _decimal(account.get("totalEquity")) + funding_total_usd
                ),
            }
        )
        rows.append(total)

    summary = _empty_row()
    summary.update(
        {
            "row_type": "account_summary",
            "id": "bybit:unified",
            "account_type": _string(account.get("accountType") or "UNIFIED"),
            "total_equity_usd": _string(account.get("totalEquity")),
            "total_wallet_balance_usd": _string(account.get("totalWalletBalance")),
            "total_margin_balance_usd": _string(account.get("totalMarginBalance")),
            "total_available_balance_usd": _string(
                account.get("totalAvailableBalance")
            ),
            "total_perp_upl_usd": _string(account.get("totalPerpUPL")),
            "total_initial_margin_usd": _string(account.get("totalInitialMargin")),
            "total_maintenance_margin_usd": _string(
                account.get("totalMaintenanceMargin")
            ),
            "account_im_rate": _string(account.get("accountIMRate")),
            "account_mm_rate": _string(account.get("accountMMRate")),
        }
    )
    rows.append(summary)

    coins = account.get("coin")
    if isinstance(coins, list):
        for item in coins:
            if not isinstance(item, dict):
                continue
            coin = _string(item.get("coin")).upper()
            if not coin:
                continue
            row = _empty_row()
            row.update(
                {
                    "row_type": "balance",
                    "id": f"bybit:balance:{coin}",
                    "account_type": _string(account.get("accountType") or "UNIFIED"),
                    "coin": coin,
                    "equity": _string(item.get("equity")),
                    "usd_value": _string(item.get("usdValue")),
                    "wallet_balance": _string(item.get("walletBalance")),
                    "locked": _string(item.get("locked")),
                    "borrow_amount": _string(item.get("borrowAmount")),
                    "spot_borrow": _string(item.get("spotBorrow")),
                    "unrealised_pnl": _string(item.get("unrealisedPnl")),
                    "cum_realised_pnl": _string(item.get("cumRealisedPnl")),
                    "margin_collateral": _string(item.get("marginCollateral")),
                    "collateral_switch": _string(item.get("collateralSwitch")),
                }
            )
            rows.append(row)

    rows.extend(funding_rows)

    for category, position in positions:
        size = _decimal(position.get("size"))
        side = _string(position.get("side"))
        symbol = _string(position.get("symbol")).upper()
        if size == 0 or not side or not symbol:
            continue
        position_idx = _string(position.get("positionIdx") or 0)
        row = _empty_row()
        row.update(
            {
                "row_type": "position",
                "id": f"bybit:position:{category}:{symbol}:{position_idx}",
                "account_type": _string(account.get("accountType") or "UNIFIED"),
                "category": category,
                "symbol": symbol,
                "side": "long" if side == "Buy" else "short",
                "position_idx": position_idx,
                "size": _string(position.get("size")),
                "position_value": _string(position.get("positionValue")),
                "avg_price": _string(position.get("avgPrice")),
                "mark_price": _string(position.get("markPrice")),
                "break_even_price": _string(position.get("breakEvenPrice")),
                "liquidation_price": _string(position.get("liqPrice")),
                "leverage": _string(position.get("leverage")),
                "position_im": _string(position.get("positionIM")),
                "position_mm": _string(position.get("positionMM")),
                "unrealised_pnl": _string(position.get("unrealisedPnl")),
                "cum_realised_pnl": _string(position.get("cumRealisedPnl")),
                "take_profit": _string(position.get("takeProfit")),
                "stop_loss": _string(position.get("stopLoss")),
                "trailing_stop": _string(position.get("trailingStop")),
                "position_status": _string(position.get("positionStatus")),
                "is_reduce_only": _string(position.get("isReduceOnly")),
                "created_at": _timestamp(position.get("createdTime")),
                "updated_at": _timestamp(position.get("updatedTime")),
            }
        )
        rows.append(row)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=BYBIT_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.post(
    "/capsule",
    summary="Create an encrypted Bybit access key",
    description=(
        "Validates a Read-only Bybit API key, encrypts it with AES-256-GCM, "
        "and returns a stateless access key. Raw credentials are not persisted."
    ),
)
async def create_bybit_capsule(request: BybitCapsuleRequest):
    api_key = request.api_key.strip()
    api_secret = request.api_secret.get_secret_value().strip()
    if not api_key or not api_secret:
        raise HTTPException(status_code=400, detail="Bybit API key and secret are required")
    base_url = _api_base_url(request.region)
    async with queued_async_client(timeout=20.0, trust_env=False) as client:
        permissions = await _validate_view_only_credentials(
            client, base_url, api_key, api_secret
        )
    capsule = encrypt_bybit_credentials(api_key, api_secret)
    return Response(
        content=json.dumps(
            {"capsule": capsule, "permissions": permissions},
            separators=(",", ":"),
        ),
        media_type="application/json",
        headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
    )


@router.get(
    "/account.csv",
    summary="Export Bybit balances and positions",
    description=(
        "Returns total Unified and Funding wallet value, their non-zero coin "
        "balances, and open linear/inverse positions using an encrypted Read-only key."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_bybit_account_csv(
    capsule: str = Query(
        ...,
        max_length=BYBIT_CAPSULE_MAX_LENGTH,
        description="Encrypted Bybit access key generated by this service.",
    ),
    region: str = Query("global", description="Bybit account API region."),
    include_positions: bool = Query(
        True, description="Include open linear and inverse derivative positions."
    ),
):
    credentials = decrypt_bybit_credentials(capsule)
    base_url = _api_base_url(region)
    async with queued_async_client(timeout=20.0, trust_env=False) as client:
        account = await _fetch_wallet_balance(
            client, base_url, credentials.api_key, credentials.api_secret
        )
        funding_balances = await _fetch_funding_balances(
            client, base_url, credentials.api_key, credentials.api_secret
        )
        spot_usd_prices = await _fetch_spot_usd_prices(client, base_url)
        positions: list[tuple[str, dict[str, Any]]] = []
        if include_positions:
            linear_usdt = await _fetch_position_category(
                client,
                base_url,
                credentials.api_key,
                credentials.api_secret,
                "linear",
                "USDT",
            )
            linear_usdc = await _fetch_position_category(
                client,
                base_url,
                credentials.api_key,
                credentials.api_secret,
                "linear",
                "USDC",
            )
            inverse = await _fetch_position_category(
                client,
                base_url,
                credentials.api_key,
                credentials.api_secret,
                "inverse",
            )
            positions.extend(("linear", item) for item in linear_usdt)
            positions.extend(("linear", item) for item in linear_usdc)
            positions.extend(("inverse", item) for item in inverse)

    return Response(
        content=_render_bybit_csv(
            account,
            positions,
            funding_balances,
            spot_usd_prices,
        ),
        media_type="text/csv",
    )
