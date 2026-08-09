import asyncio
import csv
import io
import os
import re
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response


STAKEDAO_API_URL = "https://api.stakedao.org/api"
STAKEDAO_CACHE_TTL_SECONDS = 60
STAKEDAO_STRATEGY_PROTOCOLS = ("curve", "pendle", "balancer")
STAKEDAO_CHAINS = {
    1: {
        "name": "Ethereum",
        "rpc_env": "STAKEDAO_ETHEREUM_RPC_URL",
        "rpc_url": "https://ethereum-rpc.publicnode.com",
    }
}
BALANCE_OF_SELECTOR = "70a08231"
STAKEDAO_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "position_id",
    "product",
    "protocol",
    "strategy_key",
    "name",
    "position_type",
    "position_contract",
    "asset_symbol",
    "asset_address",
    "amount",
    "price_usd",
    "value_usd",
    "apr_current_percent",
    "apr_projected_percent",
    "apr_min_percent",
    "apr_max_percent",
    "tvl_usd",
    "source_url",
]

router = APIRouter(prefix="/stakedao", tags=["stakedao"])


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


def _decimal(value: object | None) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _format_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


async def _fetch_json(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    try:
        response = await client.get(url)
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Stake DAO API request failed"
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502, detail="Stake DAO API returned invalid data"
        )
    return payload


def _strategy_targets(
    payload: dict[str, Any], protocol: str, chain_id: int
) -> list[dict[str, Any]]:
    targets = []
    deployed = payload.get("deployed")
    if not isinstance(deployed, list):
        return targets

    for value in deployed:
        strategy = _dict(value)
        if strategy.get("chainId") != chain_id:
            continue
        lp_token = _dict(strategy.get("lpToken"))
        apr = _dict(strategy.get("apr"))
        current_apr = _dict(apr.get("current")).get("total")
        projected_apr = _dict(apr.get("projected")).get("total")
        common = {
            "product": "strategy",
            "protocol": _text(strategy.get("protocol")) or protocol,
            "strategy_key": _text(strategy.get("key")),
            "name": _text(strategy.get("name")),
            "asset_symbol": _text(lp_token.get("symbol")),
            "asset_address": _text(lp_token.get("address")).lower(),
            "decimals": int(lp_token.get("decimals") or 18),
            "price_usd": strategy.get("lpPriceInUsd"),
            "apr_current_percent": current_apr,
            "apr_projected_percent": projected_apr,
            "apr_min_percent": strategy.get("minApr"),
            "apr_max_percent": strategy.get("maxApr"),
            "tvl_usd": strategy.get("tvl"),
            "source_url": (
                f"{STAKEDAO_API_URL}/strategies/{protocol}/{chain_id}.json"
            ),
        }
        sd_gauge = strategy.get("sdGauge")
        gauge_address = (
            sd_gauge if isinstance(sd_gauge, str) else _dict(sd_gauge).get("address")
        )
        for position_type, contract in (
            ("staked", gauge_address),
            ("vault", strategy.get("vault")),
        ):
            if isinstance(contract, str) and re.fullmatch(
                r"0x[0-9a-fA-F]{40}", contract
            ):
                targets.append(
                    {
                        **common,
                        "position_type": position_type,
                        "position_contract": contract.lower(),
                    }
                )
    return targets


def _locker_targets(payload: dict[str, Any], chain_id: int) -> list[dict[str, Any]]:
    targets = []
    lockers = payload.get("parsed")
    if not isinstance(lockers, list):
        return targets

    for value in lockers:
        locker = _dict(value)
        if locker.get("chainId") != chain_id:
            continue
        sd_token = _dict(locker.get("sdToken"))
        modules = _dict(locker.get("modules"))
        auto_compounder = _dict(locker.get("autoCompounder"))
        symbol = _text(sd_token.get("symbol"))
        common = {
            "product": "locker",
            "protocol": _text(locker.get("protocol")),
            "strategy_key": _text(locker.get("id")),
            "name": f"{symbol} locker" if symbol else _text(locker.get("symbol")),
            "asset_symbol": symbol,
            "asset_address": _text(sd_token.get("address")).lower(),
            "decimals": int(sd_token.get("decimals") or 18),
            "price_usd": locker.get("sdTokenPriceInUsd"),
            "apr_current_percent": "",
            "apr_projected_percent": "",
            "apr_min_percent": "",
            "apr_max_percent": "",
            "tvl_usd": locker.get("tvl"),
            "source_url": f"{STAKEDAO_API_URL}/lockers",
        }
        candidates = (
            ("staked", modules.get("gauge"), symbol),
            ("wallet", sd_token.get("address"), symbol),
            (
                "autocompounder",
                auto_compounder.get("aSdToken"),
                f"a{symbol}" if symbol else "",
            ),
        )
        for position_type, contract, asset_symbol in candidates:
            if isinstance(contract, str) and re.fullmatch(
                r"0x[0-9a-fA-F]{40}", contract
            ):
                target = {
                    **common,
                    "position_type": position_type,
                    "position_contract": contract.lower(),
                    "asset_symbol": asset_symbol,
                }
                if position_type == "autocompounder":
                    target["asset_address"] = contract.lower()
                    target["price_usd"] = None
                targets.append(target)
    return targets


async def _fetch_balances(
    client: httpx.AsyncClient,
    rpc_url: str,
    wallet: str,
    targets: list[dict[str, Any]],
) -> list[int]:
    encoded_wallet = wallet[2:].rjust(64, "0")
    payload = [
        {
            "jsonrpc": "2.0",
            "id": index,
            "method": "eth_call",
            "params": [
                {
                    "to": target["position_contract"],
                    "data": f"0x{BALANCE_OF_SELECTOR}{encoded_wallet}",
                },
                "latest",
            ],
        }
        for index, target in enumerate(targets)
    ]
    try:
        response = await client.post(rpc_url, json=payload)
        response.raise_for_status()
        items = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Stake DAO Ethereum RPC request failed"
        ) from exc
    if not isinstance(items, list):
        raise HTTPException(
            status_code=502,
            detail="Stake DAO Ethereum RPC returned invalid batch data",
        )

    by_id = {
        item.get("id"): item
        for item in items
        if isinstance(item, dict) and isinstance(item.get("id"), int)
    }
    balances = []
    for index in range(len(targets)):
        item = by_id.get(index, {})
        result = item.get("result") if isinstance(item, dict) else None
        try:
            balance = int(result, 16) if isinstance(result, str) and result != "0x" else 0
        except ValueError:
            balance = 0
        balances.append(balance)
    return balances


def _build_rows(
    wallet: str,
    chain_id: int,
    chain_name: str,
    targets: list[dict[str, Any]],
    balances: list[int],
) -> list[dict[str, str]]:
    rows = []
    for target, raw_balance in zip(targets, balances, strict=False):
        if raw_balance <= 0:
            continue
        decimals = int(target["decimals"])
        amount = Decimal(raw_balance) / (Decimal(10) ** decimals)
        price = _decimal(target.get("price_usd"))
        value_usd = amount * price if price is not None else None
        contract = _text(target.get("position_contract"))
        position_type = _text(target.get("position_type"))
        strategy_key = _text(target.get("strategy_key"))
        rows.append(
            {
                "wallet": wallet,
                "chain_id": str(chain_id),
                "chain": chain_name,
                "position_id": (
                    f"{chain_id}:{position_type}:{contract}:{strategy_key}"
                ),
                "product": _text(target.get("product")),
                "protocol": _text(target.get("protocol")),
                "strategy_key": strategy_key,
                "name": _text(target.get("name")),
                "position_type": position_type,
                "position_contract": contract,
                "asset_symbol": _text(target.get("asset_symbol")),
                "asset_address": _text(target.get("asset_address")),
                "amount": _format_decimal(amount),
                "price_usd": _format_decimal(price),
                "value_usd": _format_decimal(value_usd),
                "apr_current_percent": _format_decimal(
                    _decimal(target.get("apr_current_percent"))
                ),
                "apr_projected_percent": _format_decimal(
                    _decimal(target.get("apr_projected_percent"))
                ),
                "apr_min_percent": _format_decimal(
                    _decimal(target.get("apr_min_percent"))
                ),
                "apr_max_percent": _format_decimal(
                    _decimal(target.get("apr_max_percent"))
                ),
                "tvl_usd": _format_decimal(_decimal(target.get("tvl_usd"))),
                "source_url": _text(target.get("source_url")),
            }
        )
    return sorted(rows, key=lambda row: row["position_id"])


async def _fetch_stakedao_rows(
    wallet: str, chain_id: int
) -> list[dict[str, str]]:
    chain = STAKEDAO_CHAINS.get(chain_id)
    if chain is None:
        supported = ", ".join(str(value) for value in sorted(STAKEDAO_CHAINS))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported Stake DAO chain ID. Supported: {supported}",
        )

    async with httpx.AsyncClient(
        timeout=30.0, trust_env=False, follow_redirects=True
    ) as client:
        strategy_payloads, lockers = await asyncio.gather(
            asyncio.gather(
                *(
                    _fetch_json(
                        client,
                        f"{STAKEDAO_API_URL}/strategies/{protocol}/{chain_id}.json",
                    )
                    for protocol in STAKEDAO_STRATEGY_PROTOCOLS
                )
            ),
            _fetch_json(client, f"{STAKEDAO_API_URL}/lockers"),
        )
        targets = []
        for protocol, payload in zip(
            STAKEDAO_STRATEGY_PROTOCOLS, strategy_payloads, strict=False
        ):
            targets.extend(_strategy_targets(payload, protocol, chain_id))
        targets.extend(_locker_targets(lockers, chain_id))

        rpc_url = os.getenv(str(chain["rpc_env"])) or str(chain["rpc_url"])
        balances = await _fetch_balances(client, rpc_url, wallet, targets)

    return _build_rows(wallet, chain_id, str(chain["name"]), targets, balances)


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=STAKEDAO_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export Stake DAO positions for Google Sheets",
    description=(
        "Returns Stake DAO strategy vault/gauge and locker balances discovered "
        "through the official Stake DAO catalog and public chain RPC."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_stakedao_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(1, gt=0, description="Stake DAO network chain ID."),
):
    wallet = _normalize_wallet(address)
    rows = await _fetch_stakedao_rows(wallet, chain_id)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={
            "Cache-Control": f"public, max-age={STAKEDAO_CACHE_TTL_SECONDS}"
        },
    )
