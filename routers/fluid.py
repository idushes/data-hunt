import asyncio
import csv
import io
import re
import time
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from outbound_queue import queued_async_client


FLUID_API_URL = "https://api.fluid.io"
FLUID_LITE_ETH_API_URL = "https://api.instadapp.io"
FLUID_LITE_USD_API_URL = "https://api.fluid-lite.instadapp.ai"
FLUID_SUPPORTED_CHAINS = {
    1: "ethereum",
    137: "polygon",
    8453: "base",
    42161: "arbitrum",
    9745: "plasma",
}
FLUID_CACHE_TTL_SECONDS = 60
FLUID_CACHE_MAX_SIZE = 256
FLUID_APR_PRECISION = Decimal("0.0001")
FLUID_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "position_type",
    "position_id",
    "market",
    "contract_address",
    "supply_symbol_1",
    "supply_amount_1",
    "supply_usd_1",
    "supply_symbol_2",
    "supply_amount_2",
    "supply_usd_2",
    "borrow_symbol_1",
    "borrow_amount_1",
    "borrow_usd_1",
    "borrow_symbol_2",
    "borrow_amount_2",
    "borrow_usd_2",
    "net_usd",
    "ltv_percent",
    "liquidation_threshold_percent",
    "is_liquidated",
    "supply_apr_percent",
    "borrow_apr_percent",
    "net_apr_percent",
]

router = APIRouter(prefix="/fluid", tags=["fluid"])

_fluid_csv_cache: dict[tuple[str, int], tuple[float, str]] = {}


def _normalize_wallet(address: str) -> str:
    normalized = address.strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", normalized):
        raise HTTPException(
            status_code=400,
            detail="Address must be a 42-character EVM hex address",
        )
    return normalized.lower()


def _decimal_or_zero(value: object | None) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _format_decimal(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _scale_amount(value: object | None, decimals: object | None) -> Decimal:
    raw_value = _decimal_or_zero(value)
    try:
        decimal_places = int(str(decimals))
    except (TypeError, ValueError):
        decimal_places = 18
    return raw_value / (Decimal(10) ** decimal_places)


def _token_details(token: object | None) -> dict[str, Any] | None:
    if not isinstance(token, dict):
        return None
    address = str(token.get("address", "")).lower()
    if address == "0x0000000000000000000000000000000000000000":
        return None
    return token


def _side_tokens(side: object | None) -> list[dict[str, Any]]:
    if not isinstance(side, dict):
        return []
    return [
        token
        for token in (
            _token_details(side.get("token0")),
            _token_details(side.get("token1")),
        )
        if token is not None
    ]


def _usd_value(amount: Decimal, token: dict[str, Any]) -> Decimal:
    return amount * _decimal_or_zero(token.get("price"))


def _token_amounts_from_raw(
    raw_value: object | None,
    tokens: list[dict[str, Any]],
    dex_data: object | None,
) -> list[tuple[dict[str, Any], Decimal, Decimal]]:
    if not tokens:
        return []

    if len(tokens) == 1:
        amount = _scale_amount(raw_value, tokens[0].get("decimals"))
        return [(tokens[0], amount, _usd_value(amount, tokens[0]))]

    if not isinstance(dex_data, dict):
        return []

    shares = _decimal_or_zero(raw_value)
    amounts = []
    for index, token in enumerate(tokens[:2]):
        per_share = _decimal_or_zero(dex_data.get(f"token{index}PerShare"))
        raw_token_amount = shares * per_share / (Decimal(10) ** 18)
        amount = _scale_amount(raw_token_amount, token.get("decimals"))
        amounts.append((token, amount, _usd_value(amount, token)))
    return amounts


def _empty_row(wallet: str, chain_id: int, position_type: str) -> dict[str, str]:
    row = {column: "" for column in FLUID_CSV_HEADER}
    row.update(
        {
            "wallet": wallet,
            "chain_id": str(chain_id),
            "chain": FLUID_SUPPORTED_CHAINS[chain_id],
            "position_type": position_type,
            "is_liquidated": "false",
        }
    )
    return row


def _add_side_to_row(
    row: dict[str, str],
    side: str,
    amounts: list[tuple[dict[str, Any], Decimal, Decimal]],
) -> Decimal:
    total_usd = Decimal("0")
    for index, (token, amount, amount_usd) in enumerate(amounts[:2], start=1):
        row[f"{side}_symbol_{index}"] = str(token.get("symbol", ""))
        row[f"{side}_amount_{index}"] = _format_decimal(amount)
        row[f"{side}_usd_{index}"] = _format_decimal(amount_usd)
        total_usd += amount_usd
    return total_usd


def _format_percent(value: Decimal) -> str:
    return _format_decimal(value.quantize(FLUID_APR_PRECISION))


def _bps_to_percent(value: Decimal) -> Decimal:
    return value / Decimal("100")


def _staking_bps(token: dict[str, Any], staking_apr: dict[str, Any]) -> Decimal:
    address = str(token.get("address", "")).lower()
    return _decimal_or_zero(staking_apr.get(address))


def _rewards_bps(container: object, side: str) -> Decimal:
    rewards = container.get("rewards") if isinstance(container, dict) else None
    if not isinstance(rewards, list):
        return Decimal("0")
    return sum(
        (
            _decimal_or_zero(reward.get("rate"))
            for reward in rewards
            if isinstance(reward, dict) and reward.get("type") == side
        ),
        Decimal("0"),
    )


def _dex_share_weights(
    dex_data: object, tokens: list[dict[str, Any]]
) -> list[Decimal] | None:
    """USD split of a single DEX share between the pair's two tokens."""
    if not isinstance(dex_data, dict):
        return None

    values = []
    for index, token in enumerate(tokens[:2]):
        per_share = _decimal_or_zero(
            dex_data.get(f"token{index}PerShare")
        ) / (Decimal(10) ** 18)
        amount = _scale_amount(per_share, token.get("decimals"))
        values.append(_usd_value(amount, token))

    total = sum(values, Decimal("0"))
    if total <= 0:
        return None
    return [value / total for value in values]


def _weighted_base_bps(
    liquidity: object,
    tokens: list[dict[str, Any]],
    dex_data: object,
    staking_apr: dict[str, Any],
) -> Decimal | None:
    """Liquidity rate plus native staking yield, blended by each token's USD share."""
    if not isinstance(liquidity, dict) or not tokens:
        return None

    rates = [
        _decimal_or_zero(liquidity.get(f"token{index}")) + _staking_bps(token, staking_apr)
        for index, token in enumerate(tokens[:2])
    ]
    if len(rates) == 1:
        return rates[0]

    weights = _dex_share_weights(dex_data, tokens)
    if weights is None:
        return None
    return sum(
        (rate * weight for rate, weight in zip(rates, weights)), Decimal("0")
    )


def _vault_side_apr_bps(
    vault: dict[str, Any],
    side: str,
    tokens: list[dict[str, Any]],
    staking_apr: dict[str, Any],
) -> Decimal | None:
    rate = vault.get(f"{side}Rate")
    if not isinstance(rate, dict):
        return None

    base = _weighted_base_bps(
        rate.get("liquidity"), tokens, vault.get(f"{side}DexData"), staking_apr
    )
    if base is None:
        return None

    dex = rate.get("dex")
    trading = _decimal_or_zero(dex.get("trading")) if isinstance(dex, dict) else Decimal("0")
    rewards = _rewards_bps(vault, side)
    # Trading fees and reward emissions pay the supplier and discount the borrower.
    if side == "supply":
        return base + trading + rewards
    return base - trading - rewards


def _net_apr_bps(
    supply_usd: Decimal,
    borrow_usd: Decimal,
    supply_bps: Decimal | None,
    borrow_bps: Decimal | None,
) -> Decimal | None:
    """APR earned on the position's own equity, which is what Fluid labels Net APR."""
    if supply_bps is None:
        return None
    if borrow_usd == 0:
        return supply_bps
    if borrow_bps is None:
        return None
    net_usd = supply_usd - borrow_usd
    if net_usd == 0:
        return None
    return (supply_usd * supply_bps - borrow_usd * borrow_bps) / net_usd


def _add_rates_to_row(
    row: dict[str, str],
    supply_bps: Decimal | None,
    borrow_bps: Decimal | None,
    net_bps: Decimal | None,
) -> None:
    for column, value in (
        ("supply_apr_percent", supply_bps),
        ("borrow_apr_percent", borrow_bps),
        ("net_apr_percent", net_bps),
    ):
        if value is not None:
            row[column] = _format_percent(_bps_to_percent(value))


def _parse_lending_positions(
    wallet: str, chain_id: int, payload: object, staking_apr: dict[str, Any]
) -> list[dict[str, str]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        return []

    rows = []
    for item in payload["data"]:
        if not isinstance(item, dict):
            continue
        token = item.get("token")
        if not isinstance(token, dict):
            continue

        asset = token.get("asset")
        if not isinstance(asset, dict):
            continue
        amount = _scale_amount(item.get("totalUnderlyingAssets"), asset.get("decimals"))
        if amount <= 0:
            continue

        row = _empty_row(wallet, chain_id, "lending")
        row["position_id"] = str(token.get("address", ""))
        row["market"] = str(token.get("symbol", ""))
        row["contract_address"] = str(token.get("address", ""))
        supply_usd = _add_side_to_row(
            row,
            "supply",
            [(asset, amount, _usd_value(amount, asset))],
        )
        row["net_usd"] = _format_decimal(supply_usd)
        supply_bps = (
            _decimal_or_zero(token.get("totalRate"))
            + _staking_bps(asset, staking_apr)
            + _rewards_bps(token, "supply")
        )
        _add_rates_to_row(row, supply_bps, None, supply_bps)
        rows.append(row)
    return rows


def _parse_smart_lending_positions(
    wallet: str, chain_id: int, payload: object, staking_apr: dict[str, Any]
) -> list[dict[str, str]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        return []

    rows = []
    for item in payload["data"]:
        if not isinstance(item, dict):
            continue
        token = item.get("token")
        if not isinstance(token, dict):
            continue
        token_pair = token.get("tokens")
        tokens = _side_tokens(token_pair)
        if not tokens:
            continue

        amounts = []
        for index, position_token in enumerate(tokens[:2]):
            amount = _scale_amount(
                item.get(f"underlyingAssetsToken{index}"),
                position_token.get("decimals"),
            )
            amounts.append(
                (position_token, amount, _usd_value(amount, position_token))
            )
        if not any(amount > 0 for _, amount, _ in amounts):
            continue

        row = _empty_row(wallet, chain_id, "smart_lending")
        row["position_id"] = str(token.get("address", ""))
        row["market"] = "-".join(str(token.get("symbol", "")) for token in tokens)
        row["contract_address"] = str(token.get("address", ""))
        supply_usd = _add_side_to_row(row, "supply", amounts)
        row["net_usd"] = _format_decimal(supply_usd)

        rate = token.get("rate")
        supply_bps = None
        if isinstance(rate, dict):
            base = _weighted_base_bps(
                rate.get("liquidity"), tokens, token.get("dex"), staking_apr
            )
            if base is not None:
                dex = rate.get("dex")
                trading = (
                    _decimal_or_zero(dex.get("trading"))
                    if isinstance(dex, dict)
                    else Decimal("0")
                )
                supply_bps = base + trading + _rewards_bps(token, "supply")
        _add_rates_to_row(row, supply_bps, None, supply_bps)
        rows.append(row)
    return rows


def _parse_lite_eth_position(
    wallet: str, chain_id: int, payload: object
) -> list[dict[str, str]]:
    if chain_id != 1 or not isinstance(payload, list):
        return []

    vault = next(
        (
            item
            for item in payload
            if isinstance(item, dict) and str(item.get("version")) == "2"
        ),
        None,
    )
    if not isinstance(vault, dict):
        return []

    amount = _decimal_or_zero(vault.get("userSupplyAmount"))
    if amount <= 0:
        return []

    asset = vault.get("token")
    if not isinstance(asset, dict):
        return []

    row = _empty_row(wallet, chain_id, "lite_eth")
    row["position_id"] = str(vault.get("vault", ""))
    row["market"] = "Fluid Lite ETH"
    row["contract_address"] = str(vault.get("vault", ""))
    supply_usd = _add_side_to_row(
        row,
        "supply",
        [(asset, amount, _usd_value(amount, asset))],
    )
    row["net_usd"] = _format_decimal(supply_usd)

    apy = vault.get("apy")
    if isinstance(apy, dict) and apy.get("apyWithoutFee") is not None:
        # Fluid Lite reports a ready-made percentage rather than basis points.
        supply_bps = _decimal_or_zero(apy.get("apyWithoutFee")) * Decimal("100")
        _add_rates_to_row(row, supply_bps, None, supply_bps)
    return [row]


def _parse_lite_usd_position(
    wallet: str,
    chain_id: int,
    vault_payload: object,
    user_payload: object,
) -> list[dict[str, str]]:
    if chain_id != 1:
        return []
    if not isinstance(vault_payload, dict) or not vault_payload.get("success"):
        return []
    if not isinstance(user_payload, dict) or not user_payload.get("success"):
        return []

    vault = vault_payload.get("data")
    user = user_payload.get("data")
    if not isinstance(vault, dict) or not isinstance(user, dict):
        return []
    asset = vault.get("underlyingAsset")
    if not isinstance(asset, dict):
        return []

    amount = _scale_amount(user.get("assets"), asset.get("decimals"))
    if amount <= 0:
        return []

    normalized_asset = {
        **asset,
        "price": asset.get("price", "0"),
    }
    row = _empty_row(wallet, chain_id, "lite_usdc")
    row["position_id"] = str(vault.get("address", ""))
    row["market"] = str(vault.get("symbol", "Fluid Lite USD"))
    row["contract_address"] = str(vault.get("address", ""))
    supply_usd = _add_side_to_row(
        row,
        "supply",
        [(normalized_asset, amount, _usd_value(amount, normalized_asset))],
    )
    row["net_usd"] = _format_decimal(supply_usd)
    supply_bps = _decimal_or_zero(vault.get("rate"))
    _add_rates_to_row(row, supply_bps, None, supply_bps)
    return [row]


def _parse_vault_positions(
    wallet: str, chain_id: int, payload: object, staking_apr: dict[str, Any]
) -> list[dict[str, str]]:
    if not isinstance(payload, list):
        return []

    rows = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        vault = item.get("vault")
        if not isinstance(vault, dict):
            continue

        supply_tokens = _side_tokens(vault.get("supplyToken"))
        borrow_tokens = _side_tokens(vault.get("borrowToken"))
        supply_amounts = _token_amounts_from_raw(
            item.get("supply"), supply_tokens, vault.get("supplyDexData")
        )
        borrow_amounts = _token_amounts_from_raw(
            item.get("borrow"), borrow_tokens, vault.get("borrowDexData")
        )
        if not any(amount > 0 for _, amount, _ in [*supply_amounts, *borrow_amounts]):
            continue

        row = _empty_row(wallet, chain_id, "vault")
        row["position_id"] = str(item.get("id", ""))
        supply_label = "-".join(
            str(token.get("symbol", "")) for token in supply_tokens
        )
        borrow_label = "-".join(
            str(token.get("symbol", "")) for token in borrow_tokens
        )
        row["market"] = f"{supply_label}/{borrow_label}".strip("/")
        row["contract_address"] = str(vault.get("address", ""))
        row["is_liquidated"] = str(bool(item.get("isLiquidated"))).lower()

        supply_usd = _add_side_to_row(row, "supply", supply_amounts)
        borrow_usd = _add_side_to_row(row, "borrow", borrow_amounts)
        row["net_usd"] = _format_decimal(supply_usd - borrow_usd)
        if supply_usd > 0:
            row["ltv_percent"] = _format_decimal(
                borrow_usd / supply_usd * Decimal("100")
            )
        row["liquidation_threshold_percent"] = _format_decimal(
            _decimal_or_zero(vault.get("liquidationThreshold")) / Decimal("100")
        )

        supply_bps = _vault_side_apr_bps(vault, "supply", supply_tokens, staking_apr)
        borrow_bps = _vault_side_apr_bps(vault, "borrow", borrow_tokens, staking_apr)
        _add_rates_to_row(
            row,
            supply_bps,
            borrow_bps,
            _net_apr_bps(supply_usd, borrow_usd, supply_bps, borrow_bps),
        )
        rows.append(row)
    return rows


async def _fetch_json(client: httpx.AsyncClient, path: str) -> object:
    return await _fetch_json_url(client, f"{FLUID_API_URL}{path}")


async def _fetch_json_url(client: httpx.AsyncClient, url: str) -> object:
    try:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Fluid API returned HTTP {exc.response.status_code}",
        ) from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Fluid API request failed"
        ) from exc


async def _fetch_fluid_rows(
    client: httpx.AsyncClient, wallet: str, chain_id: int
) -> list[dict[str, str]]:
    core_requests = [
        _fetch_json(client, f"/v2/lending/{chain_id}/users/{wallet}/positions"),
        _fetch_json(client, f"/v2/{chain_id}/users/{wallet}/nfts"),
        _fetch_json(
            client, f"/v2/smart-lending/{chain_id}/users/{wallet}/positions"
        ),
        _fetch_json(client, f"/{chain_id}/staking-apr"),
    ]
    if chain_id == 1:
        core_requests.extend(
            [
                _fetch_json_url(
                    client,
                    f"{FLUID_LITE_ETH_API_URL}/v2/mainnet/lite/users/{wallet}/vaults",
                ),
                _fetch_json_url(client, f"{FLUID_LITE_USD_API_URL}/lite-usd/vault"),
                _fetch_json_url(
                    client,
                    f"{FLUID_LITE_USD_API_URL}/lite-usd/vault/user/{wallet}",
                ),
            ]
        )

    payloads = await asyncio.gather(*core_requests)
    lending, vaults, smart_lending, staking = payloads[:4]
    staking_apr = {
        str(address).lower(): rate
        for address, rate in (staking.items() if isinstance(staking, dict) else ())
    }
    rows = [
        *_parse_lending_positions(wallet, chain_id, lending, staking_apr),
        *_parse_vault_positions(wallet, chain_id, vaults, staking_apr),
        *_parse_smart_lending_positions(wallet, chain_id, smart_lending, staking_apr),
    ]
    if chain_id == 1:
        lite_eth, lite_usd_vault, lite_usd_user = payloads[4:]
        rows.extend(_parse_lite_eth_position(wallet, chain_id, lite_eth))
        rows.extend(
            _parse_lite_usd_position(
                wallet, chain_id, lite_usd_vault, lite_usd_user
            )
        )
    return rows


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(FLUID_CSV_HEADER)
    for row in rows:
        writer.writerow([row.get(column, "") for column in FLUID_CSV_HEADER])
    return output.getvalue()


def _get_cached_csv(wallet: str, chain_id: int) -> str | None:
    cached = _fluid_csv_cache.get((wallet, chain_id))
    if cached is None:
        return None
    cached_at, content = cached
    if time.monotonic() - cached_at >= FLUID_CACHE_TTL_SECONDS:
        _fluid_csv_cache.pop((wallet, chain_id), None)
        return None
    return content


def _set_cached_csv(wallet: str, chain_id: int, content: str) -> None:
    key = (wallet, chain_id)
    if key in _fluid_csv_cache:
        _fluid_csv_cache.pop(key)
    elif len(_fluid_csv_cache) >= FLUID_CACHE_MAX_SIZE:
        oldest_key = min(_fluid_csv_cache, key=lambda item: _fluid_csv_cache[item][0])
        _fluid_csv_cache.pop(oldest_key)
    _fluid_csv_cache[key] = (time.monotonic(), content)


@router.get(
    "/positions.csv",
    summary="Export Fluid positions for Google Sheets",
    description=(
        "Returns normalized Fluid lending, vault borrowing, and smart-lending "
        "positions for an EVM wallet. Suitable for Google Sheets IMPORTDATA."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_fluid_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(1, description="Fluid network chain ID."),
):
    wallet = _normalize_wallet(address)
    if chain_id not in FLUID_SUPPORTED_CHAINS:
        supported = ", ".join(str(value) for value in FLUID_SUPPORTED_CHAINS)
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported Fluid chain ID. Supported: {supported}",
        )

    cached = _get_cached_csv(wallet, chain_id)
    if cached is None:
        async with queued_async_client(timeout=20.0) as client:
            rows = await _fetch_fluid_rows(client, wallet, chain_id)
        cached = _render_csv(rows)
        _set_cached_csv(wallet, chain_id, cached)

    return Response(
        content=cached,
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={FLUID_CACHE_TTL_SECONDS}"},
    )
