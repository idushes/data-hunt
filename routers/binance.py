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

from binance_capsule import (
    BINANCE_CAPSULE_MAX_LENGTH,
    decrypt_binance_credentials,
    encrypt_binance_credentials,
)
from outbound_queue import queued_async_client


BINANCE_SPOT_BASE_URL = "https://api.binance.com"
BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"
BINANCE_RECV_WINDOW = 5000
BINANCE_CSV_HEADER = [
    "row_type",
    "id",
    "account_type",
    "asset",
    "symbol",
    "side",
    "position_side",
    "total_equity_usd",
    "total_wallet_balance_usd",
    "total_margin_balance_usd",
    "total_available_balance_usd",
    "total_unrealized_pnl_usd",
    "free",
    "locked",
    "total",
    "usd_value",
    "wallet_balance",
    "margin_balance",
    "available_balance",
    "cross_wallet_balance",
    "cross_unrealized_pnl",
    "position_amount",
    "notional",
    "entry_price",
    "break_even_price",
    "mark_price",
    "liquidation_price",
    "leverage",
    "margin_type",
    "isolated_margin",
    "unrealized_pnl",
    "update_time",
]

router = APIRouter(prefix="/binance", tags=["binance"])


class BinanceCapsuleRequest(BaseModel):
    api_key: str
    api_secret: SecretStr


def _signed_query(
    api_secret: str,
    params: dict[str, object] | None = None,
    timestamp_ms: int | None = None,
) -> str:
    signed_params = list((params or {}).items())
    signed_params.extend(
        [
            ("recvWindow", BINANCE_RECV_WINDOW),
            ("timestamp", timestamp_ms or int(time.time() * 1000)),
        ]
    )
    payload = urlencode([(key, str(value)) for key, value in signed_params])
    signature = hmac.new(
        api_secret.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    return f"{payload}&signature={signature}"


def _binance_error(payload: object, fallback: str) -> str:
    if isinstance(payload, dict):
        message = payload.get("msg")
        if isinstance(message, str) and message:
            return message
    return fallback


async def _get_signed_json(
    client: httpx.AsyncClient,
    base_url: str,
    path: str,
    api_key: str,
    api_secret: str,
    params: dict[str, object] | None = None,
) -> object:
    response = await client.get(
        f"{base_url}{path}",
        params=_signed_query(api_secret, params),
        headers={"X-MBX-APIKEY": api_key, "Accept": "application/json"},
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Binance returned invalid JSON") from exc

    if response.status_code >= 400:
        status_code = response.status_code
        if status_code not in {400, 401, 403, 418, 429}:
            status_code = 502
        raise HTTPException(
            status_code=status_code,
            detail=f"Binance API error: {_binance_error(payload, f'HTTP {response.status_code}')}",
        )
    return payload


async def _get_public_json(
    client: httpx.AsyncClient,
    base_url: str,
    path: str,
) -> object:
    response = await client.get(
        f"{base_url}{path}", headers={"Accept": "application/json"}
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Binance returned invalid JSON") from exc
    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"Binance API error: {_binance_error(payload, f'HTTP {response.status_code}')}",
        )
    return payload


async def _validate_read_only_credentials(
    client: httpx.AsyncClient,
    api_key: str,
    api_secret: str,
) -> dict[str, object]:
    payload = await _get_signed_json(
        client,
        BINANCE_SPOT_BASE_URL,
        "/sapi/v1/account/apiRestrictions",
        api_key,
        api_secret,
    )
    if not isinstance(payload, dict) or payload.get("enableReading") is not True:
        raise HTTPException(status_code=400, detail="Binance API key must allow reading")

    write_permissions = (
        "enableWithdrawals",
        "enableInternalTransfer",
        "permitsUniversalTransfer",
        "enableSpotAndMarginTrading",
        "enableMargin",
        "enableFutures",
        "enableVanillaOptions",
        "enablePortfolioMarginTrading",
    )
    enabled = [name for name in write_permissions if payload.get(name) is True]
    if enabled:
        raise HTTPException(
            status_code=400,
            detail="Binance API key must be read-only; disable trading and withdrawals",
        )
    return {"read_only": True}


async def _fetch_spot_account(
    client: httpx.AsyncClient,
    api_key: str,
    api_secret: str,
) -> dict[str, Any]:
    payload = await _get_signed_json(
        client,
        BINANCE_SPOT_BASE_URL,
        "/api/v3/account",
        api_key,
        api_secret,
        {"omitZeroBalances": "true"},
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("balances"), list):
        raise HTTPException(status_code=502, detail="Invalid Binance Spot response")
    return payload


async def _fetch_spot_prices(client: httpx.AsyncClient) -> dict[str, Decimal]:
    payload = await _get_public_json(
        client, BINANCE_SPOT_BASE_URL, "/api/v3/ticker/price"
    )
    if not isinstance(payload, list):
        raise HTTPException(status_code=502, detail="Invalid Binance price response")
    prices: dict[str, Decimal] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        price = _decimal(item.get("price"))
        if symbol and price > 0:
            prices[symbol] = price
    return prices


async def _fetch_futures_account(
    client: httpx.AsyncClient,
    api_key: str,
    api_secret: str,
) -> dict[str, Any]:
    payload = await _get_signed_json(
        client,
        BINANCE_FUTURES_BASE_URL,
        "/fapi/v3/account",
        api_key,
        api_secret,
    )
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="Invalid Binance Futures response")
    return payload


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


def _decimal_string(value: Decimal) -> str:
    return format(value.normalize(), "f") if value else "0"


def _timestamp(value: object | None) -> str:
    milliseconds = int(_decimal(value))
    if milliseconds <= 0:
        return ""
    try:
        return datetime.fromtimestamp(milliseconds / 1000, tz=UTC).isoformat()
    except (OverflowError, OSError, ValueError):
        return ""


def _empty_row() -> dict[str, str]:
    return {column: "" for column in BINANCE_CSV_HEADER}


def _asset_usd_value(
    asset: str,
    amount: Decimal,
    prices: dict[str, Decimal],
) -> Decimal | None:
    if asset == "USDT":
        return amount
    direct_price = prices.get(f"{asset}USDT")
    if direct_price is not None:
        return amount * direct_price
    for quote_asset in ("USDC", "FDUSD", "BTC"):
        asset_price = prices.get(f"{asset}{quote_asset}")
        quote_price = prices.get(f"{quote_asset}USDT")
        if asset_price is not None and quote_price is not None:
            return amount * asset_price * quote_price
    return None


def _render_binance_csv(
    spot_account: dict[str, Any],
    prices: dict[str, Decimal],
    futures_account: dict[str, Any] | None,
) -> str:
    rows: list[dict[str, str]] = []
    spot_rows: list[dict[str, str]] = []
    spot_total = Decimal(0)

    for balance in spot_account.get("balances", []):
        if not isinstance(balance, dict):
            continue
        asset = _string(balance.get("asset")).upper()
        free = _decimal(balance.get("free"))
        locked = _decimal(balance.get("locked"))
        total = free + locked
        if not asset or total == 0:
            continue
        usd_value = _asset_usd_value(asset, total, prices)
        if usd_value is not None:
            spot_total += usd_value
        row = _empty_row()
        row.update(
            {
                "row_type": "spot_balance",
                "id": f"binance:spot:balance:{asset}",
                "account_type": "spot",
                "asset": asset,
                "free": _decimal_string(free),
                "locked": _decimal_string(locked),
                "total": _decimal_string(total),
                "usd_value": _decimal_string(usd_value) if usd_value is not None else "",
                "update_time": _timestamp(spot_account.get("updateTime")),
            }
        )
        spot_rows.append(row)

    summary = _empty_row()
    summary.update(
        {
            "row_type": "account_summary",
            "id": "binance:spot",
            "account_type": "spot",
            "total_equity_usd": _decimal_string(spot_total),
            "update_time": _timestamp(spot_account.get("updateTime")),
        }
    )
    rows.append(summary)
    rows.extend(spot_rows)

    if futures_account is not None:
        futures_summary = _empty_row()
        futures_summary.update(
            {
                "row_type": "account_summary",
                "id": "binance:futures:usdm",
                "account_type": "usdm_futures",
                "total_wallet_balance_usd": _string(
                    futures_account.get("totalWalletBalance")
                ),
                "total_margin_balance_usd": _string(
                    futures_account.get("totalMarginBalance")
                ),
                "total_available_balance_usd": _string(
                    futures_account.get("availableBalance")
                ),
                "total_unrealized_pnl_usd": _string(
                    futures_account.get("totalUnrealizedProfit")
                ),
                "update_time": _timestamp(futures_account.get("updateTime")),
            }
        )
        rows.append(futures_summary)

        for balance in futures_account.get("assets", []):
            if not isinstance(balance, dict):
                continue
            asset = _string(balance.get("asset")).upper()
            wallet_balance = _decimal(balance.get("walletBalance"))
            margin_balance = _decimal(balance.get("marginBalance"))
            if not asset or wallet_balance == 0 and margin_balance == 0:
                continue
            row = _empty_row()
            row.update(
                {
                    "row_type": "futures_balance",
                    "id": f"binance:futures:balance:{asset}",
                    "account_type": "usdm_futures",
                    "asset": asset,
                    "wallet_balance": _string(balance.get("walletBalance")),
                    "margin_balance": _string(balance.get("marginBalance")),
                    "available_balance": _string(balance.get("availableBalance")),
                    "cross_wallet_balance": _string(
                        balance.get("crossWalletBalance")
                    ),
                    "cross_unrealized_pnl": _string(
                        balance.get("crossUnPnl")
                    ),
                    "unrealized_pnl": _string(balance.get("unrealizedProfit")),
                    "update_time": _timestamp(balance.get("updateTime")),
                }
            )
            rows.append(row)

        for position in futures_account.get("positions", []):
            if not isinstance(position, dict):
                continue
            amount = _decimal(position.get("positionAmt"))
            symbol = _string(position.get("symbol")).upper()
            if not symbol or amount == 0:
                continue
            position_side = _string(position.get("positionSide") or "BOTH").upper()
            side = "long" if amount > 0 else "short"
            row = _empty_row()
            row.update(
                {
                    "row_type": "futures_position",
                    "id": f"binance:futures:position:{symbol}:{position_side}",
                    "account_type": "usdm_futures",
                    "symbol": symbol,
                    "side": side,
                    "position_side": position_side.lower(),
                    "position_amount": _string(position.get("positionAmt")),
                    "notional": _string(position.get("notional")),
                    "entry_price": _string(position.get("entryPrice")),
                    "break_even_price": _string(position.get("breakEvenPrice")),
                    "mark_price": _string(position.get("markPrice")),
                    "liquidation_price": _string(position.get("liquidationPrice")),
                    "leverage": _string(position.get("leverage")),
                    "margin_type": _string(position.get("marginType")),
                    "isolated_margin": _string(position.get("isolatedMargin")),
                    "unrealized_pnl": _string(position.get("unrealizedProfit")),
                    "update_time": _timestamp(position.get("updateTime")),
                }
            )
            rows.append(row)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=BINANCE_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.post(
    "/capsule",
    summary="Create an encrypted Binance access key",
    description=(
        "Validates a read-only Binance API key, encrypts it with AES-256-GCM, "
        "and returns a stateless access key. Raw credentials are not persisted."
    ),
)
async def create_binance_capsule(request: BinanceCapsuleRequest):
    api_key = request.api_key.strip()
    api_secret = request.api_secret.get_secret_value().strip()
    if not api_key or not api_secret:
        raise HTTPException(
            status_code=400, detail="Binance API key and secret are required"
        )
    async with queued_async_client(timeout=20.0, trust_env=False) as client:
        permissions = await _validate_read_only_credentials(client, api_key, api_secret)
    capsule = encrypt_binance_credentials(api_key, api_secret)
    return Response(
        content=json.dumps(
            {"capsule": capsule, "permissions": permissions}, separators=(",", ":")
        ),
        media_type="application/json",
        headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
    )


@router.get(
    "/account.csv",
    summary="Export Binance balances and positions",
    description=(
        "Returns non-zero Spot balances with estimated USD values and, when "
        "available, USD-M Futures balances and open positions."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_binance_account_csv(
    capsule: str = Query(
        ...,
        max_length=BINANCE_CAPSULE_MAX_LENGTH,
        description="Encrypted Binance access key generated by this service.",
    ),
    include_futures: bool = Query(
        True, description="Include USD-M Futures balances and open positions."
    ),
):
    credentials = decrypt_binance_credentials(capsule)
    async with queued_async_client(timeout=20.0, trust_env=False) as client:
        spot_account = await _fetch_spot_account(
            client, credentials.api_key, credentials.api_secret
        )
        prices = await _fetch_spot_prices(client)
        futures_account: dict[str, Any] | None = None
        if include_futures:
            try:
                futures_account = await _fetch_futures_account(
                    client, credentials.api_key, credentials.api_secret
                )
            except HTTPException as exc:
                if exc.status_code not in {400, 401, 403}:
                    raise

    return Response(
        content=_render_binance_csv(spot_account, prices, futures_account),
        media_type="text/csv",
    )
