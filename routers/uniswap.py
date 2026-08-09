import csv
import io
import os
import re
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from web3 import Web3


UNISWAP_CACHE_TTL_SECONDS = 60
UNISWAP_MAX_POSITIONS = 200
UNISWAP_V3_POSITION_MANAGER = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
UNISWAP_CHAINS = {
    1: {
        "name": "Ethereum",
        "rpc_env": "UNISWAP_ETHEREUM_RPC_URL",
        "rpc_url": "https://ethereum-rpc.publicnode.com",
        "position_manager": UNISWAP_V3_POSITION_MANAGER,
        "factory": UNISWAP_V3_FACTORY,
    }
}
USD_STABLECOINS = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    "0xdc035d45d973e3ec169d2276ddab16f1e407384f",  # USDS
}
Q96 = Decimal(2**96)

UNISWAP_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "protocol",
    "position_manager",
    "position_id",
    "token_id",
    "pool_address",
    "fee_tier",
    "tick_lower",
    "tick_upper",
    "current_tick",
    "in_range",
    "is_closed",
    "liquidity",
    "token0_symbol",
    "token0_name",
    "token0_address",
    "token0_amount",
    "token0_owed",
    "token1_symbol",
    "token1_name",
    "token1_address",
    "token1_amount",
    "token1_owed",
    "price_token1_per_token0",
    "price_lower",
    "price_upper",
    "value_usd",
]

POSITION_MANAGER_ABI = [
    {
        "inputs": [{"type": "address"}],
        "name": "balanceOf",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"type": "address"}, {"type": "uint256"}],
        "name": "tokenOfOwnerByIndex",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"type": "uint256"}],
        "name": "positions",
        "outputs": [
            {"name": "nonce", "type": "uint96"},
            {"name": "operator", "type": "address"},
            {"name": "token0", "type": "address"},
            {"name": "token1", "type": "address"},
            {"name": "fee", "type": "uint24"},
            {"name": "tickLower", "type": "int24"},
            {"name": "tickUpper", "type": "int24"},
            {"name": "liquidity", "type": "uint128"},
            {"name": "feeGrowthInside0LastX128", "type": "uint256"},
            {"name": "feeGrowthInside1LastX128", "type": "uint256"},
            {"name": "tokensOwed0", "type": "uint128"},
            {"name": "tokensOwed1", "type": "uint128"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

FACTORY_ABI = [
    {
        "inputs": [
            {"type": "address"},
            {"type": "address"},
            {"type": "uint24"},
        ],
        "name": "getPool",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    }
]

POOL_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint8"},
            {"name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

ERC20_ABI = [
    {
        "inputs": [],
        "name": "symbol",
        "outputs": [{"type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "name",
        "outputs": [{"type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]

router = APIRouter(prefix="/uniswap", tags=["uniswap"])


def _normalize_wallet(address: str) -> str:
    normalized = address.strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", normalized):
        raise HTTPException(
            status_code=400,
            detail="Address must be a 42-character EVM hex address",
        )
    return normalized.lower()


def _decimal_or_zero(value: object | None) -> Decimal:
    try:
        return Decimal(str(value)) if value is not None else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _format_decimal(value: Decimal, places: int = 18) -> str:
    if value == 0:
        return "0"
    with localcontext() as context:
        context.prec = 80
        quantum = Decimal(1).scaleb(-places)
        normalized = value.quantize(quantum).normalize()
    return format(normalized, "f")


def _sqrt_price_at_tick(tick: int) -> Decimal:
    with localcontext() as context:
        context.prec = 80
        return Decimal("1.0001") ** (Decimal(tick) / Decimal(2))


def _position_amounts(
    liquidity: int,
    tick_lower: int,
    tick_upper: int,
    sqrt_price_x96: int,
    decimals0: int,
    decimals1: int,
) -> tuple[Decimal, Decimal]:
    if liquidity <= 0:
        return Decimal(0), Decimal(0)

    with localcontext() as context:
        context.prec = 80
        current = Decimal(sqrt_price_x96) / Q96
        lower = _sqrt_price_at_tick(tick_lower)
        upper = _sqrt_price_at_tick(tick_upper)
        amount0_raw = Decimal(0)
        amount1_raw = Decimal(0)

        if current <= lower:
            amount0_raw = Decimal(liquidity) * (upper - lower) / (lower * upper)
        elif current < upper:
            amount0_raw = Decimal(liquidity) * (upper - current) / (current * upper)
            amount1_raw = Decimal(liquidity) * (current - lower)
        else:
            amount1_raw = Decimal(liquidity) * (upper - lower)

        return (
            amount0_raw / (Decimal(10) ** decimals0),
            amount1_raw / (Decimal(10) ** decimals1),
        )


def _human_price(sqrt_price_x96: int, decimals0: int, decimals1: int) -> Decimal:
    with localcontext() as context:
        context.prec = 80
        raw_price = (Decimal(sqrt_price_x96) / Q96) ** 2
        return raw_price * (Decimal(10) ** (decimals0 - decimals1))


def _tick_price(tick: int, decimals0: int, decimals1: int) -> Decimal:
    with localcontext() as context:
        context.prec = 80
        return (Decimal("1.0001") ** tick) * (
            Decimal(10) ** (decimals0 - decimals1)
        )


def _usd_value(
    token0_address: str,
    token1_address: str,
    amount0: Decimal,
    amount1: Decimal,
    price_token1_per_token0: Decimal,
) -> Decimal | None:
    token0_is_usd = token0_address.lower() in USD_STABLECOINS
    token1_is_usd = token1_address.lower() in USD_STABLECOINS
    if token1_is_usd:
        return amount1 + amount0 * price_token1_per_token0
    if token0_is_usd and price_token1_per_token0 > 0:
        return amount0 + amount1 / price_token1_per_token0
    return None


async def _rpc_batch_calls(
    client: httpx.AsyncClient,
    rpc_url: str,
    calls: list[tuple[str, Any]],
) -> list[tuple[Any, ...] | None]:
    if not calls:
        return []

    payload = [
        {
            "jsonrpc": "2.0",
            "id": index,
            "method": "eth_call",
            "params": [
                {
                    "to": Web3.to_checksum_address(address),
                    "data": function._encode_transaction_data(),
                },
                "latest",
            ],
        }
        for index, (address, function) in enumerate(calls)
    ]
    try:
        response = await client.post(rpc_url, json=payload)
        response.raise_for_status()
        items = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502,
            detail="Uniswap Ethereum RPC request failed",
        ) from exc

    if not isinstance(items, list):
        raise HTTPException(
            status_code=502,
            detail="Uniswap Ethereum RPC returned an invalid batch response",
        )

    by_id = {
        item.get("id"): item
        for item in items
        if isinstance(item, dict) and isinstance(item.get("id"), int)
    }
    codec = Web3().codec
    decoded: list[tuple[Any, ...] | None] = []
    for index, (_, function) in enumerate(calls):
        item = by_id.get(index, {})
        result = item.get("result") if isinstance(item, dict) else None
        if not isinstance(result, str) or not result.startswith("0x"):
            decoded.append(None)
            continue
        output_types = [output["type"] for output in function.abi.get("outputs", [])]
        try:
            decoded.append(codec.decode(output_types, bytes.fromhex(result[2:])))
        except Exception:
            decoded.append(None)

    return decoded


def _required_call(result: tuple[Any, ...] | None, detail: str) -> tuple[Any, ...]:
    if result is None:
        raise HTTPException(status_code=502, detail=detail)
    return result


async def _fetch_uniswap_rows(
    wallet: str,
    chain_id: int,
    include_closed: bool,
) -> list[dict[str, str]]:
    chain = UNISWAP_CHAINS.get(chain_id)
    if chain is None:
        supported = ", ".join(str(value) for value in sorted(UNISWAP_CHAINS))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported Uniswap chain ID. Supported: {supported}",
        )

    rpc_url = os.getenv(str(chain["rpc_env"])) or str(chain["rpc_url"])
    w3 = Web3()
    manager_address = Web3.to_checksum_address(str(chain["position_manager"]))
    manager = w3.eth.contract(address=manager_address, abi=POSITION_MANAGER_ABI)
    factory = w3.eth.contract(
        address=Web3.to_checksum_address(str(chain["factory"])), abi=FACTORY_ABI
    )
    checksum_wallet = Web3.to_checksum_address(wallet)

    async with httpx.AsyncClient(timeout=20.0, trust_env=False) as client:
        count_result = _required_call(
            (
                await _rpc_batch_calls(
                    client,
                    rpc_url,
                    [
                        (
                            manager_address,
                            manager.functions.balanceOf(checksum_wallet),
                        )
                    ],
                )
            )[0],
            "Failed to read Uniswap position count",
        )
        count = int(count_result[0])
        if count > UNISWAP_MAX_POSITIONS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Wallet has {count} Uniswap positions; maximum supported is "
                    f"{UNISWAP_MAX_POSITIONS}"
                ),
            )

        token_id_results = await _rpc_batch_calls(
            client,
            rpc_url,
            [
                (
                    manager_address,
                    manager.functions.tokenOfOwnerByIndex(checksum_wallet, index),
                )
                for index in range(count)
            ],
        )
        token_ids = [
            int(_required_call(result, "Failed to read Uniswap token ID")[0])
            for result in token_id_results
        ]
        raw_position_results = await _rpc_batch_calls(
            client,
            rpc_url,
            [
                (manager_address, manager.functions.positions(token_id))
                for token_id in token_ids
            ],
        )
        raw_positions = [
            _required_call(result, f"Failed to read Uniswap token ID {token_id}")
            for token_id, result in zip(token_ids, raw_position_results, strict=False)
        ]

        positions = []
        for token_id, position in zip(token_ids, raw_positions, strict=False):
            liquidity = int(position[7])
            tokens_owed0 = int(position[10])
            tokens_owed1 = int(position[11])
            if (
                not include_closed
                and liquidity == 0
                and tokens_owed0 == 0
                and tokens_owed1 == 0
            ):
                continue
            positions.append((int(token_id), position))

        token_addresses = sorted(
            {
                str(position[index]).lower()
                for _, position in positions
                for index in (2, 3)
            }
        )
        token_contracts = {
            address: w3.eth.contract(
                address=Web3.to_checksum_address(address), abi=ERC20_ABI
            )
            for address in token_addresses
        }
        metadata_calls = [
            (address, token_contracts[address].functions.symbol())
            for address in token_addresses
        ] + [
            (address, token_contracts[address].functions.name())
            for address in token_addresses
        ] + [
            (address, token_contracts[address].functions.decimals())
            for address in token_addresses
        ]
        metadata_results = await _rpc_batch_calls(client, rpc_url, metadata_calls)
        token_count = len(token_addresses)
        metadata = {}
        for index, address in enumerate(token_addresses):
            symbol_result = metadata_results[index]
            name_result = metadata_results[token_count + index]
            decimals_result = metadata_results[(2 * token_count) + index]
            metadata[address] = {
                "address": address,
                "symbol": str(symbol_result[0]) if symbol_result else address[:10],
                "name": str(name_result[0]) if name_result else "",
                "decimals": int(decimals_result[0]) if decimals_result else 18,
            }

        pool_keys = sorted(
            {
                (str(position[2]).lower(), str(position[3]).lower(), int(position[4]))
                for _, position in positions
            }
        )
        pool_results = await _rpc_batch_calls(
            client,
            rpc_url,
            [
                (
                    str(chain["factory"]),
                    factory.functions.getPool(
                        Web3.to_checksum_address(token0_address),
                        Web3.to_checksum_address(token1_address),
                        fee,
                    ),
                )
                for token0_address, token1_address, fee in pool_keys
            ],
        )
        pool_by_key = {
            key: str(result[0]).lower()
            for key, result in zip(pool_keys, pool_results, strict=False)
            if result is not None and int(str(result[0]), 16) != 0
        }
        unique_pools = sorted(set(pool_by_key.values()))
        pool_contracts = {
            pool_address: w3.eth.contract(
                address=Web3.to_checksum_address(pool_address), abi=POOL_ABI
            )
            for pool_address in unique_pools
        }
        slot0_results = await _rpc_batch_calls(
            client,
            rpc_url,
            [
                (pool_address, pool_contracts[pool_address].functions.slot0())
                for pool_address in unique_pools
            ],
        )
        slot0_by_pool = {
            pool_address: _required_call(
                result, f"Failed to read Uniswap pool {pool_address}"
            )
            for pool_address, result in zip(
                unique_pools, slot0_results, strict=False
            )
        }

    rows: list[dict[str, str]] = []
    for token_id, position in positions:
        token0_address = str(position[2]).lower()
        token1_address = str(position[3]).lower()
        fee = int(position[4])
        tick_lower = int(position[5])
        tick_upper = int(position[6])
        liquidity = int(position[7])
        token0 = metadata[token0_address]
        token1 = metadata[token1_address]
        pool_address = pool_by_key.get((token0_address, token1_address, fee))
        if not pool_address:
            continue
        slot0 = slot0_by_pool[pool_address]

        sqrt_price_x96 = int(slot0[0])
        current_tick = int(slot0[1])
        amount0, amount1 = _position_amounts(
            liquidity,
            tick_lower,
            tick_upper,
            sqrt_price_x96,
            token0["decimals"],
            token1["decimals"],
        )
        price = _human_price(
            sqrt_price_x96, token0["decimals"], token1["decimals"]
        )
        value_usd = _usd_value(
            token0_address, token1_address, amount0, amount1, price
        )
        rows.append(
            {
                "wallet": wallet,
                "chain_id": str(chain_id),
                "chain": str(chain["name"]),
                "protocol": "Uniswap V3",
                "position_manager": manager_address.lower(),
                "position_id": f"{chain_id}:{manager_address.lower()}:{token_id}",
                "token_id": str(token_id),
                "pool_address": str(pool_address).lower(),
                "fee_tier": _format_decimal(Decimal(fee) / Decimal(10000), 4),
                "tick_lower": str(tick_lower),
                "tick_upper": str(tick_upper),
                "current_tick": str(current_tick),
                "in_range": str(tick_lower <= current_tick < tick_upper).lower(),
                "is_closed": str(liquidity == 0).lower(),
                "liquidity": str(liquidity),
                "token0_symbol": token0["symbol"],
                "token0_name": token0["name"],
                "token0_address": token0_address,
                "token0_amount": _format_decimal(amount0),
                "token0_owed": _format_decimal(
                    Decimal(position[10]) / (Decimal(10) ** token0["decimals"])
                ),
                "token1_symbol": token1["symbol"],
                "token1_name": token1["name"],
                "token1_address": token1_address,
                "token1_amount": _format_decimal(amount1),
                "token1_owed": _format_decimal(
                    Decimal(position[11]) / (Decimal(10) ** token1["decimals"])
                ),
                "price_token1_per_token0": _format_decimal(price),
                "price_lower": _format_decimal(
                    _tick_price(tick_lower, token0["decimals"], token1["decimals"])
                ),
                "price_upper": _format_decimal(
                    _tick_price(tick_upper, token0["decimals"], token1["decimals"])
                ),
                "value_usd": _format_decimal(value_usd, 8) if value_usd is not None else "",
            }
        )

    return sorted(rows, key=lambda row: int(row["token_id"]))


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=UNISWAP_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export Uniswap V3 positions for Google Sheets",
    description=(
        "Returns current token amounts, price range and status for Uniswap V3 "
        "NFT liquidity positions owned by an EVM wallet."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_uniswap_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(1, gt=0, description="Uniswap network chain ID."),
    include_closed: bool = Query(
        False,
        description="Include closed NFT positions with zero liquidity.",
    ),
):
    wallet = _normalize_wallet(address)
    rows = await _fetch_uniswap_rows(wallet, chain_id, include_closed)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={UNISWAP_CACHE_TTL_SECONDS}"},
    )
