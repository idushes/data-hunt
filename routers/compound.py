import csv
import io
import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from eth_abi import decode, encode
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from web3 import Web3

from outbound_queue import queued_async_client


COMPOUND_CACHE_TTL_SECONDS = 60
SECONDS_PER_YEAR = Decimal(31_536_000)
FACTOR_SCALE = Decimal(10**18)


@dataclass(frozen=True)
class CometMarket:
    name: str
    address: str


@dataclass(frozen=True)
class CompoundChain:
    name: str
    rpc_env: str
    rpc_url: str
    markets: tuple[CometMarket, ...]


COMPOUND_CHAINS = {
    1: CompoundChain(
        "Ethereum",
        "COMPOUND_ETHEREUM_RPC_URL",
        "https://ethereum-rpc.publicnode.com",
        (
            CometMarket("USDC", "0xc3d688B66703497DAA19211EEdff47f25384cdc3"),
            CometMarket("USDS", "0x5D409e56D886231aDAf00c8775665AD0f9897b56"),
            CometMarket("USDT", "0x3Afdc9BCA9213A35503b077a6072F3D0d5AB0840"),
            CometMarket("WBTC", "0xe85Dc543813B8c2CFEaAc371517b925a166a9293"),
            CometMarket("WETH", "0xA17581A9E3356d9A858b789D68B4d866e593aE94"),
            CometMarket("wstETH", "0x3D0bb1ccaB520A66e607822fC55BC921738fAFE3"),
        ),
    ),
    8453: CompoundChain(
        "Base",
        "COMPOUND_BASE_RPC_URL",
        "https://base-rpc.publicnode.com",
        (
            CometMarket("AERO", "0x784efeB622244d2348d4F2522f8860B96fbEcE89"),
            CometMarket("USDbC", "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf"),
            CometMarket("USDC", "0xb125E6687d4313864e53df431d5425969c15Eb2F"),
            CometMarket("USDS", "0x2c776041CCFe903071AF44aa147368a9c8EEA518"),
            CometMarket("WETH", "0x46e6b214b524310239732D51387075E0e70970bf"),
        ),
    ),
    42161: CompoundChain(
        "Arbitrum",
        "COMPOUND_ARBITRUM_RPC_URL",
        "https://arbitrum-one-rpc.publicnode.com",
        (
            CometMarket("USDC.e", "0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA"),
            CometMarket("USDC", "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf"),
            CometMarket("USDT", "0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07"),
            CometMarket("WETH", "0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486"),
        ),
    ),
}

COMPOUND_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "protocol",
    "market",
    "market_address",
    "position_id",
    "position_type",
    "token_address",
    "token_symbol",
    "balance",
    "balance_usd",
    "supply_amount",
    "supply_usd",
    "borrow_amount",
    "borrow_usd",
    "supply_apy_percent",
    "borrow_apy_percent",
    "is_liquidatable",
    "net_usd",
]

ASSET_INFO_TYPES = [
    "uint8",
    "address",
    "address",
    "uint64",
    "uint64",
    "uint64",
    "uint64",
    "uint128",
]

router = APIRouter(prefix="/compound", tags=["compound"])


def _normalize_wallet(address: str) -> str:
    normalized = address.strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", normalized):
        raise HTTPException(
            status_code=400,
            detail="Address must be a 42-character EVM hex address",
        )
    return normalized.lower()


def _selector(signature: str) -> bytes:
    return Web3.keccak(text=signature)[:4]


def _call_data(signature: str, types: list[str] | None = None, values=None) -> str:
    payload = _selector(signature)
    if types:
        payload += encode(types, values or [])
    return "0x" + payload.hex()


def _decode_result(result: object, types: list[str]):
    if not isinstance(result, str) or not result.startswith("0x"):
        raise ValueError("missing eth_call result")
    return decode(types, bytes.fromhex(result[2:]))


async def _rpc_batch(
    client: httpx.AsyncClient,
    rpc_url: str,
    calls: list[tuple[str, str, list[str]]],
) -> list[tuple[Any, ...]]:
    payload = [
        {
            "jsonrpc": "2.0",
            "id": index,
            "method": "eth_call",
            "params": [{"to": address, "data": data}, "latest"],
        }
        for index, (address, data, _types) in enumerate(calls)
    ]
    try:
        response = await client.post(rpc_url, json=payload)
        response.raise_for_status()
        values = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(status_code=502, detail="Compound RPC request failed") from exc
    if not isinstance(values, list):
        raise HTTPException(status_code=502, detail="Compound RPC returned invalid data")
    by_id = {
        item.get("id"): item
        for item in values
        if isinstance(item, dict) and isinstance(item.get("id"), int)
    }
    decoded = []
    for index, (_address, _data, output_types) in enumerate(calls):
        item = by_id.get(index, {})
        try:
            decoded.append(_decode_result(item.get("result"), output_types))
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=502,
                detail="Compound RPC returned an undecodable contract result",
            ) from exc
    return decoded


def _decimal(value: object | None) -> Decimal:
    try:
        return Decimal(str(value)) if value is not None else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _format(value: Decimal) -> str:
    return "0" if value == 0 else format(value.normalize(), "f")


def _amount(raw: int, scale: int) -> Decimal:
    return Decimal(raw) / Decimal(scale)


def _usd(amount: Decimal, raw_price: int, price_scale: int) -> Decimal:
    if price_scale <= 0:
        return Decimal(0)
    return amount * Decimal(raw_price) / Decimal(price_scale)


def _apy(raw_rate: int) -> Decimal:
    return Decimal(raw_rate) * SECONDS_PER_YEAR * Decimal(100) / FACTOR_SCALE


def _base_row(
    wallet: str,
    chain_id: int,
    chain: CompoundChain,
    market: CometMarket,
) -> dict[str, str]:
    row = {column: "" for column in COMPOUND_CSV_HEADER}
    row.update(
        {
            "wallet": wallet,
            "chain_id": str(chain_id),
            "chain": chain.name,
            "protocol": "Compound III",
            "market": market.name,
            "market_address": market.address.lower(),
        }
    )
    return row


async def _fetch_market_rows(
    client: httpx.AsyncClient,
    rpc_url: str,
    wallet: str,
    chain_id: int,
    chain: CompoundChain,
    market: CometMarket,
) -> list[dict[str, str]]:
    comet = market.address
    wallet_arg = [Web3.to_checksum_address(wallet)]
    first_calls = [
        (comet, _call_data("baseToken()"), ["address"]),
        (comet, _call_data("baseScale()"), ["uint256"]),
        (comet, _call_data("priceScale()"), ["uint256"]),
        (comet, _call_data("baseTokenPriceFeed()"), ["address"]),
        (comet, _call_data("numAssets()"), ["uint8"]),
        (
            comet,
            _call_data("balanceOf(address)", ["address"], wallet_arg),
            ["uint256"],
        ),
        (
            comet,
            _call_data("borrowBalanceOf(address)", ["address"], wallet_arg),
            ["uint256"],
        ),
        (comet, _call_data("getUtilization()"), ["uint256"]),
        (
            comet,
            _call_data("isLiquidatable(address)", ["address"], wallet_arg),
            ["bool"],
        ),
    ]
    first = await _rpc_batch(client, rpc_url, first_calls)
    base_token = str(first[0][0]).lower()
    base_scale = int(first[1][0])
    price_scale = int(first[2][0])
    base_feed = str(first[3][0])
    num_assets = int(first[4][0])
    supply_raw = int(first[5][0])
    borrow_raw = int(first[6][0])
    utilization = int(first[7][0])
    is_liquidatable = bool(first[8][0])

    asset_calls = [
        (
            comet,
            _call_data("getAssetInfo(uint8)", ["uint8"], [index]),
            ASSET_INFO_TYPES,
        )
        for index in range(num_assets)
    ]
    asset_info_values = await _rpc_batch(client, rpc_url, asset_calls)
    asset_infos = [
        {
            "address": str(value[1]).lower(),
            "price_feed": str(value[2]),
            "scale": int(value[3]),
        }
        for value in asset_info_values
    ]

    second_calls = [
        (
            comet,
            _call_data("getPrice(address)", ["address"], [base_feed]),
            ["uint128"],
        ),
        (
            comet,
            _call_data("getSupplyRate(uint256)", ["uint256"], [utilization]),
            ["uint64"],
        ),
        (
            comet,
            _call_data("getBorrowRate(uint256)", ["uint256"], [utilization]),
            ["uint64"],
        ),
    ]
    for asset in asset_infos:
        second_calls.extend(
            [
                (
                    comet,
                    _call_data(
                        "collateralBalanceOf(address,address)",
                        ["address", "address"],
                        [wallet, asset["address"]],
                    ),
                    ["uint128"],
                ),
                (
                    comet,
                    _call_data(
                        "getPrice(address)",
                        ["address"],
                        [asset["price_feed"]],
                    ),
                    ["uint128"],
                ),
            ]
        )
    second = await _rpc_batch(client, rpc_url, second_calls)
    base_price = int(second[0][0])
    supply_rate = int(second[1][0])
    borrow_rate = int(second[2][0])

    rows = []
    supply_amount = _amount(supply_raw, base_scale)
    borrow_amount = _amount(borrow_raw, base_scale)
    supply_usd = _usd(supply_amount, base_price, price_scale)
    borrow_usd = _usd(borrow_amount, base_price, price_scale)
    if supply_raw or borrow_raw:
        row = _base_row(wallet, chain_id, chain, market)
        row.update(
            {
                "position_id": f"{market.address.lower()}:base",
                "position_type": "base",
                "token_address": base_token,
                "token_symbol": market.name,
                "balance": _format(supply_amount - borrow_amount),
                "balance_usd": _format(supply_usd - borrow_usd),
                "supply_amount": _format(supply_amount),
                "supply_usd": _format(supply_usd),
                "borrow_amount": _format(borrow_amount),
                "borrow_usd": _format(borrow_usd),
                "supply_apy_percent": _format(_apy(supply_rate)),
                "borrow_apy_percent": _format(_apy(borrow_rate)),
                "is_liquidatable": str(is_liquidatable).lower(),
                "net_usd": _format(supply_usd - borrow_usd),
            }
        )
        rows.append(row)

    for index, asset in enumerate(asset_infos):
        balance_raw = int(second[3 + index * 2][0])
        if balance_raw == 0:
            continue
        price_raw = int(second[4 + index * 2][0])
        balance = _amount(balance_raw, int(asset["scale"]))
        balance_usd = _usd(balance, price_raw, price_scale)
        row = _base_row(wallet, chain_id, chain, market)
        row.update(
            {
                "position_id": (
                    f"{market.address.lower()}:collateral:{asset['address']}"
                ),
                "position_type": "collateral",
                "token_address": asset["address"],
                "balance": _format(balance),
                "balance_usd": _format(balance_usd),
                "is_liquidatable": str(is_liquidatable).lower(),
                "net_usd": _format(balance_usd),
            }
        )
        rows.append(row)
    return rows


async def _fetch_compound_rows(
    client: httpx.AsyncClient, wallet: str, chain_id: int
) -> list[dict[str, str]]:
    chain = COMPOUND_CHAINS.get(chain_id)
    if chain is None:
        supported = ", ".join(str(value) for value in sorted(COMPOUND_CHAINS))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported Compound chain. Supported chain IDs: {supported}",
        )
    rpc_url = os.getenv(chain.rpc_env) or chain.rpc_url
    rows = []
    for market in chain.markets:
        rows.extend(
            await _fetch_market_rows(
                client, rpc_url, wallet, chain_id, chain, market
            )
        )
    return sorted(rows, key=lambda row: row["position_id"])


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=COMPOUND_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export Compound III positions",
    description=(
        "Reads active base supply/borrow and collateral balances directly from "
        "official Compound III Comet markets."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_compound_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(1, description="1, 8453, or 42161."),
):
    wallet = _normalize_wallet(address)
    async with queued_async_client(timeout=45.0, trust_env=False) as client:
        rows = await _fetch_compound_rows(client, wallet, chain_id)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={COMPOUND_CACHE_TTL_SECONDS}"},
    )
