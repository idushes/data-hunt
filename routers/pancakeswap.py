from typing import Any

from fastapi import APIRouter, Query
from fastapi.responses import Response

from routers.uniswap import (
    ETHEREUM_USD_STABLECOINS,
    UNISWAP_CSV_HEADER,
    _fetch_v3_rows,
    _normalize_wallet,
    _render_csv,
)


PANCAKESWAP_CACHE_TTL_SECONDS = 60
PANCAKESWAP_V3_FACTORY = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
PANCAKESWAP_V3_POSITION_MANAGER = "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364"
PANCAKESWAP_POOL_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint32"},
            {"name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]
BNB_USD_STABLECOINS = {
    "0x55d398326f99059ff775485246999027b3197955",  # Binance-Peg USDT
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # Binance-Peg USDC
}
PANCAKESWAP_CHAINS: dict[int, dict[str, Any]] = {
    1: {
        "name": "Ethereum",
        "rpc_env": "PANCAKESWAP_ETHEREUM_RPC_URL",
        "rpc_url": "https://ethereum-rpc.publicnode.com",
        "position_manager": PANCAKESWAP_V3_POSITION_MANAGER,
        "factory": PANCAKESWAP_V3_FACTORY,
        "pool_abi": PANCAKESWAP_POOL_ABI,
        "usd_stablecoins": ETHEREUM_USD_STABLECOINS,
    },
    56: {
        "name": "BNB Chain",
        "rpc_env": "PANCAKESWAP_BNB_RPC_URL",
        "rpc_url": "https://bsc-dataseed.bnbchain.org",
        "position_manager": PANCAKESWAP_V3_POSITION_MANAGER,
        "factory": PANCAKESWAP_V3_FACTORY,
        "pool_abi": PANCAKESWAP_POOL_ABI,
        "usd_stablecoins": BNB_USD_STABLECOINS,
    },
}
PANCAKESWAP_CSV_HEADER = UNISWAP_CSV_HEADER.copy()

router = APIRouter(prefix="/pancakeswap", tags=["pancakeswap"])


async def _fetch_pancakeswap_rows(
    wallet: str,
    chain_id: int,
    include_closed: bool,
) -> list[dict[str, str]]:
    return await _fetch_v3_rows(
        wallet,
        chain_id,
        include_closed,
        PANCAKESWAP_CHAINS,
        "PancakeSwap V3",
    )


@router.get(
    "/positions.csv",
    summary="Export PancakeSwap V3 positions for Google Sheets",
    description=(
        "Returns current token amounts, price range, status, USD value, and "
        "claimable fees for PancakeSwap V3 NFT liquidity positions."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_pancakeswap_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(56, gt=0, description="PancakeSwap network chain ID."),
    include_closed: bool = Query(
        False,
        description="Include closed NFT positions with zero liquidity.",
    ),
):
    wallet = _normalize_wallet(address)
    rows = await _fetch_pancakeswap_rows(wallet, chain_id, include_closed)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={PANCAKESWAP_CACHE_TTL_SECONDS}"},
    )
