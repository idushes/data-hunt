import csv
import io
import os
from decimal import Decimal
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from web3 import Web3

from outbound_queue import queued_async_client
from routers.stablecoins import ARBITRUM_TOKENS, BASE_TOKENS, ETHEREUM_TOKENS
from routers.uniswap import (
    ERC20_ABI,
    _format_decimal,
    _human_price,
    _normalize_wallet,
    _position_amounts,
    _rpc_batch_calls,
    _tick_price,
    _usd_value,
)


UNISWAP_V4_CACHE_TTL_SECONDS = 60
UNISWAP_V4_MAX_POSITIONS = 200
Q128_INT = 2**128
UINT256_MODULUS = 2**256
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _stablecoin_addresses(tokens: tuple[dict[str, Any], ...]) -> set[str]:
    return {str(token["address"]).lower() for token in tokens}


UNISWAP_V4_CHAINS = {
    1: {
        "name": "Ethereum",
        "native_symbol": "ETH",
        "native_name": "Ether",
        "rpc_env": "UNISWAP_ETHEREUM_RPC_URL",
        "rpc_url": "https://ethereum-rpc.publicnode.com",
        "blockscout_url": "https://eth.blockscout.com/api/v2",
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "position_manager": "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e",
        "usd_stablecoins": _stablecoin_addresses(ETHEREUM_TOKENS),
    },
    42161: {
        "name": "Arbitrum",
        "native_symbol": "ETH",
        "native_name": "Ether",
        "rpc_env": "UNISWAP_ARBITRUM_RPC_URL",
        "rpc_url": "https://arbitrum-one-rpc.publicnode.com",
        "blockscout_url": "https://arbitrum.blockscout.com/api/v2",
        "pool_manager": "0x360E68faCcca8cA495c1B759Fd9EEe466db9FB32",
        "position_manager": "0xd88F38F930b7952f2DB2432Cb002E7abbF3dD869",
        "usd_stablecoins": _stablecoin_addresses(ARBITRUM_TOKENS),
    },
    8453: {
        "name": "Base",
        "native_symbol": "ETH",
        "native_name": "Ether",
        "rpc_env": "UNISWAP_BASE_RPC_URL",
        "rpc_url": "https://base-rpc.publicnode.com",
        "blockscout_url": "https://base.blockscout.com/api/v2",
        "pool_manager": "0x498581fF718922c3f8e6A244956aF099B2652b2b",
        "position_manager": "0x7C5f5A4bBd8fD63184577525326123B519429bDc",
        "usd_stablecoins": _stablecoin_addresses(BASE_TOKENS),
    },
}

UNISWAP_V4_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "protocol",
    "position_manager",
    "pool_manager",
    "position_id",
    "token_id",
    "pool_id",
    "hooks_address",
    "fee_tier",
    "tick_spacing",
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
    "token1_symbol",
    "token1_name",
    "token1_address",
    "token1_amount",
    "token0_fees_claimable",
    "token1_fees_claimable",
    "fees_value_usd",
    "price_token1_per_token0",
    "price_lower",
    "price_upper",
    "value_usd",
]

POSITION_MANAGER_V4_ABI = [
    {
        "inputs": [{"type": "uint256"}],
        "name": "ownerOf",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"type": "uint256"}],
        "name": "getPoolAndPositionInfo",
        "outputs": [
            {
                "name": "poolKey",
                "type": "tuple",
                "components": [
                    {"name": "currency0", "type": "address"},
                    {"name": "currency1", "type": "address"},
                    {"name": "fee", "type": "uint24"},
                    {"name": "tickSpacing", "type": "int24"},
                    {"name": "hooks", "type": "address"},
                ],
            },
            {"name": "info", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

EXTSLOAD_ABI = [
    {
        "inputs": [{"name": "slots", "type": "bytes32[]"}],
        "name": "extsload",
        "outputs": [{"name": "values", "type": "bytes32[]"}],
        "stateMutability": "view",
        "type": "function",
    }
]

router = APIRouter(prefix="/uniswap/v4", tags=["uniswap-v4"])


def _signed_24(value: int) -> int:
    value &= 0xFFFFFF
    return value - (1 << 24) if value & (1 << 23) else value


def _packed_int24(value: int) -> bytes:
    return (value % (1 << 24)).to_bytes(3, "big")


def _slot(value: int) -> bytes:
    return (value % UINT256_MODULUS).to_bytes(32, "big")


def _pool_id(pool_key: tuple[Any, ...]) -> bytes:
    encoded = Web3().codec.encode(
        ["address", "address", "uint24", "int24", "address"],
        list(pool_key),
    )
    return bytes(Web3.keccak(encoded))


def _position_ticks(info: int) -> tuple[int, int]:
    return _signed_24(info >> 8), _signed_24(info >> 32)


def _position_state_slots(
    pool_id: bytes,
    position_manager: str,
    token_id: int,
    tick_lower: int,
    tick_upper: int,
) -> list[bytes]:
    pool_state_slot = int.from_bytes(Web3.keccak(pool_id + _slot(6)), "big")
    ticks_mapping_slot = pool_state_slot + 4

    def tick_slot(tick: int) -> int:
        encoded_tick = (tick % UINT256_MODULUS).to_bytes(32, "big")
        return int.from_bytes(
            Web3.keccak(encoded_tick + _slot(ticks_mapping_slot)), "big"
        )

    position_key = Web3.keccak(
        bytes.fromhex(position_manager[2:])
        + _packed_int24(tick_lower)
        + _packed_int24(tick_upper)
        + _slot(token_id)
    )
    positions_mapping_slot = pool_state_slot + 6
    position_slot = int.from_bytes(
        Web3.keccak(bytes(position_key) + _slot(positions_mapping_slot)), "big"
    )
    lower_slot = tick_slot(tick_lower)
    upper_slot = tick_slot(tick_upper)
    return [
        _slot(pool_state_slot),
        _slot(pool_state_slot + 1),
        _slot(pool_state_slot + 2),
        _slot(lower_slot + 1),
        _slot(lower_slot + 2),
        _slot(upper_slot + 1),
        _slot(upper_slot + 2),
        _slot(position_slot),
        _slot(position_slot + 1),
        _slot(position_slot + 2),
    ]


def _decode_position_state(values: list[bytes]) -> dict[str, int]:
    if len(values) != 10:
        raise HTTPException(status_code=502, detail="Uniswap V4 state is incomplete")
    words = [int.from_bytes(value, "big") for value in values]
    slot0 = words[0]
    current_tick = _signed_24(slot0 >> 160)
    tick_lower_outside0, tick_lower_outside1 = words[3], words[4]
    tick_upper_outside0, tick_upper_outside1 = words[5], words[6]

    return {
        "sqrt_price_x96": slot0 & ((1 << 160) - 1),
        "current_tick": current_tick,
        "lp_fee": (slot0 >> 208) & 0xFFFFFF,
        "fee_growth_global0": words[1],
        "fee_growth_global1": words[2],
        "lower_outside0": tick_lower_outside0,
        "lower_outside1": tick_lower_outside1,
        "upper_outside0": tick_upper_outside0,
        "upper_outside1": tick_upper_outside1,
        "liquidity": words[7] & ((1 << 128) - 1),
        "fee_growth_last0": words[8],
        "fee_growth_last1": words[9],
    }


def _claimable_fees_raw(
    state: dict[str, int], tick_lower: int, tick_upper: int
) -> tuple[int, int]:
    current_tick = state["current_tick"]
    if current_tick < tick_lower:
        inside0 = state["lower_outside0"] - state["upper_outside0"]
        inside1 = state["lower_outside1"] - state["upper_outside1"]
    elif current_tick >= tick_upper:
        inside0 = state["upper_outside0"] - state["lower_outside0"]
        inside1 = state["upper_outside1"] - state["lower_outside1"]
    else:
        inside0 = (
            state["fee_growth_global0"]
            - state["lower_outside0"]
            - state["upper_outside0"]
        )
        inside1 = (
            state["fee_growth_global1"]
            - state["lower_outside1"]
            - state["upper_outside1"]
        )
    liquidity = state["liquidity"]
    fees0 = ((inside0 - state["fee_growth_last0"]) % UINT256_MODULUS)
    fees1 = ((inside1 - state["fee_growth_last1"]) % UINT256_MODULUS)
    return fees0 * liquidity // Q128_INT, fees1 * liquidity // Q128_INT


async def _discover_token_ids(
    client: httpx.AsyncClient,
    blockscout_url: str,
    position_manager: str,
    wallet: str,
) -> list[int]:
    url = f"{blockscout_url}/tokens/{position_manager}/instances"
    params: dict[str, str] = {"holder_address_hash": wallet}
    token_ids: set[int] = set()

    while True:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise HTTPException(
                status_code=502, detail="Uniswap V4 position discovery failed"
            ) from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise HTTPException(
                status_code=502,
                detail="Uniswap V4 position discovery returned invalid data",
            )
        for item in payload["items"]:
            if not isinstance(item, dict):
                continue
            try:
                token_ids.add(int(item.get("id")))
            except (TypeError, ValueError):
                continue
        if len(token_ids) > UNISWAP_V4_MAX_POSITIONS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Wallet has more than {UNISWAP_V4_MAX_POSITIONS} Uniswap V4 "
                    "positions"
                ),
            )
        next_page = payload.get("next_page_params")
        if not isinstance(next_page, dict) or not next_page:
            return sorted(token_ids)
        params = {
            "holder_address_hash": wallet,
            **{str(key): str(value) for key, value in next_page.items()},
        }


def _native_metadata(chain: dict[str, Any]) -> dict[str, Any]:
    return {
        "address": ZERO_ADDRESS,
        "symbol": str(chain["native_symbol"]),
        "name": str(chain["native_name"]),
        "decimals": 18,
    }


async def _fetch_uniswap_v4_rows(
    wallet: str,
    chain_id: int,
    include_closed: bool,
) -> list[dict[str, str]]:
    chain = UNISWAP_V4_CHAINS.get(chain_id)
    if chain is None:
        supported = ", ".join(str(value) for value in sorted(UNISWAP_V4_CHAINS))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported Uniswap V4 chain ID. Supported: {supported}",
        )

    rpc_url = os.getenv(str(chain["rpc_env"])) or str(chain["rpc_url"])
    position_manager_address = str(chain["position_manager"]).lower()
    pool_manager_address = str(chain["pool_manager"]).lower()
    w3 = Web3()
    position_manager = w3.eth.contract(
        address=Web3.to_checksum_address(position_manager_address),
        abi=POSITION_MANAGER_V4_ABI,
    )
    pool_manager = w3.eth.contract(
        address=Web3.to_checksum_address(pool_manager_address), abi=EXTSLOAD_ABI
    )

    async with queued_async_client(timeout=25.0, trust_env=False) as client:
        token_ids = await _discover_token_ids(
            client,
            str(chain["blockscout_url"]),
            position_manager_address,
            wallet,
        )
        position_calls = [
            (
                position_manager_address,
                position_manager.functions.ownerOf(token_id),
            )
            for token_id in token_ids
        ] + [
            (
                position_manager_address,
                position_manager.functions.getPoolAndPositionInfo(token_id),
            )
            for token_id in token_ids
        ]
        position_results = await _rpc_batch_calls(client, rpc_url, position_calls)
        count = len(token_ids)
        positions: list[dict[str, Any]] = []
        for index, token_id in enumerate(token_ids):
            owner_result = position_results[index]
            info_result = position_results[count + index]
            if (
                owner_result is None
                or str(owner_result[0]).lower() != wallet
                or info_result is None
            ):
                continue
            pool_key = tuple(info_result[0])
            info = int(info_result[1])
            tick_lower, tick_upper = _position_ticks(info)
            pool_id = _pool_id(pool_key)
            positions.append(
                {
                    "token_id": token_id,
                    "pool_key": pool_key,
                    "pool_id": pool_id,
                    "tick_lower": tick_lower,
                    "tick_upper": tick_upper,
                }
            )

        state_results = await _rpc_batch_calls(
            client,
            rpc_url,
            [
                (
                    pool_manager_address,
                    pool_manager.functions.extsload(
                        _position_state_slots(
                            position["pool_id"],
                            position_manager_address,
                            position["token_id"],
                            position["tick_lower"],
                            position["tick_upper"],
                        )
                    ),
                )
                for position in positions
            ],
        )
        active_positions: list[dict[str, Any]] = []
        for position, result in zip(positions, state_results, strict=False):
            if result is None or not isinstance(result[0], (list, tuple)):
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to read Uniswap V4 token ID {position['token_id']}",
                )
            state = _decode_position_state(list(result[0]))
            if include_closed or state["liquidity"] > 0:
                active_positions.append({**position, "state": state})

        token_addresses = sorted(
            {
                str(position["pool_key"][index]).lower()
                for position in active_positions
                for index in (0, 1)
                if str(position["pool_key"][index]).lower() != ZERO_ADDRESS
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

    metadata = {ZERO_ADDRESS: _native_metadata(chain)}
    token_count = len(token_addresses)
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

    rows: list[dict[str, str]] = []
    for position in active_positions:
        pool_key = position["pool_key"]
        token0_address = str(pool_key[0]).lower()
        token1_address = str(pool_key[1]).lower()
        token0 = metadata[token0_address]
        token1 = metadata[token1_address]
        state = position["state"]
        liquidity = state["liquidity"]
        tick_lower = position["tick_lower"]
        tick_upper = position["tick_upper"]
        amount0, amount1 = _position_amounts(
            liquidity,
            tick_lower,
            tick_upper,
            state["sqrt_price_x96"],
            token0["decimals"],
            token1["decimals"],
        )
        price = _human_price(
            state["sqrt_price_x96"], token0["decimals"], token1["decimals"]
        )
        value_usd = _usd_value(
            token0_address,
            token1_address,
            amount0,
            amount1,
            price,
            chain["usd_stablecoins"],
        )
        fees0_raw, fees1_raw = _claimable_fees_raw(state, tick_lower, tick_upper)
        fees0 = Decimal(fees0_raw) / (Decimal(10) ** token0["decimals"])
        fees1 = Decimal(fees1_raw) / (Decimal(10) ** token1["decimals"])
        fees_value_usd = _usd_value(
            token0_address,
            token1_address,
            fees0,
            fees1,
            price,
            chain["usd_stablecoins"],
        )
        token_id = position["token_id"]
        rows.append(
            {
                "wallet": wallet,
                "chain_id": str(chain_id),
                "chain": str(chain["name"]),
                "protocol": "Uniswap V4",
                "position_manager": position_manager_address,
                "pool_manager": pool_manager_address,
                "position_id": f"{chain_id}:{position_manager_address}:{token_id}",
                "token_id": str(token_id),
                "pool_id": "0x" + position["pool_id"].hex(),
                "hooks_address": str(pool_key[4]).lower(),
                "fee_tier": _format_decimal(
                    Decimal(state["lp_fee"]) / Decimal(10000), 4
                ),
                "tick_spacing": str(int(pool_key[3])),
                "tick_lower": str(tick_lower),
                "tick_upper": str(tick_upper),
                "current_tick": str(state["current_tick"]),
                "in_range": str(
                    tick_lower <= state["current_tick"] < tick_upper
                ).lower(),
                "is_closed": str(liquidity == 0).lower(),
                "liquidity": str(liquidity),
                "token0_symbol": token0["symbol"],
                "token0_name": token0["name"],
                "token0_address": token0_address,
                "token0_amount": _format_decimal(amount0),
                "token1_symbol": token1["symbol"],
                "token1_name": token1["name"],
                "token1_address": token1_address,
                "token1_amount": _format_decimal(amount1),
                "token0_fees_claimable": _format_decimal(fees0),
                "token1_fees_claimable": _format_decimal(fees1),
                "fees_value_usd": (
                    _format_decimal(fees_value_usd, 8)
                    if fees_value_usd is not None
                    else ""
                ),
                "price_token1_per_token0": _format_decimal(price),
                "price_lower": _format_decimal(
                    _tick_price(tick_lower, token0["decimals"], token1["decimals"])
                ),
                "price_upper": _format_decimal(
                    _tick_price(tick_upper, token0["decimals"], token1["decimals"])
                ),
                "value_usd": (
                    _format_decimal(value_usd, 8) if value_usd is not None else ""
                ),
            }
        )
    return sorted(rows, key=lambda row: int(row["token_id"]))


def _render_v4_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=UNISWAP_V4_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export Uniswap V4 positions for Google Sheets",
    description=(
        "Returns current token amounts, price range, status, and claimable fees "
        "for Uniswap V4 NFT liquidity positions owned by an EVM wallet."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_uniswap_v4_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(1, gt=0, description="Uniswap V4 network chain ID."),
    include_closed: bool = Query(
        False,
        description="Include owned NFT positions with zero liquidity.",
    ),
):
    wallet = _normalize_wallet(address)
    rows = await _fetch_uniswap_v4_rows(wallet, chain_id, include_closed)
    return Response(
        content=_render_v4_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={UNISWAP_V4_CACHE_TTL_SECONDS}"},
    )
