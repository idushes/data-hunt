import asyncio
import csv
import io
import os
import re
from decimal import Decimal
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from outbound_queue import queued_async_client

from routers.solana import SOLANA_RPC_ENDPOINT, _is_solana_address, _solana_rpc_request


STABLECOINS_CACHE_TTL_SECONDS = 60
ETHEREUM_RPC_ENDPOINT = "https://ethereum-rpc.publicnode.com"
ARBITRUM_RPC_ENDPOINT = "https://arbitrum-one-rpc.publicnode.com"
BASE_RPC_ENDPOINT = "https://base-rpc.publicnode.com"
BALANCE_OF_SELECTOR = "70a08231"
STABLECOIN_CSV_HEADER = [
    "balance_id",
    "wallet",
    "network",
    "chain_id",
    "token_symbol",
    "token_name",
    "token_address",
    "balance",
    "decimals",
]
ETHEREUM_TOKENS = (
    {
        "symbol": "USDC",
        "name": "USD Coin",
        "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "decimals": 6,
    },
    {
        "symbol": "USDT",
        "name": "Tether USD",
        "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "decimals": 6,
    },
)
ARBITRUM_TOKENS = (
    {
        "symbol": "USDC",
        "name": "USD Coin",
        "address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "decimals": 6,
    },
    {
        "symbol": "USDT",
        "name": "Tether USD",
        "address": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
        "decimals": 6,
    },
)
BASE_TOKENS = (
    {
        "symbol": "USDC",
        "name": "USD Coin",
        "address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "decimals": 6,
    },
    {
        "symbol": "USDT",
        "name": "Bridged Tether USD",
        "address": "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2",
        "decimals": 6,
    },
)
EVM_CHAINS = {
    1: {
        "name": "Ethereum",
        "rpc_env": "STABLECOINS_ETHEREUM_RPC_URL",
        "rpc_url": ETHEREUM_RPC_ENDPOINT,
        "tokens": ETHEREUM_TOKENS,
    },
    42161: {
        "name": "Arbitrum",
        "rpc_env": "STABLECOINS_ARBITRUM_RPC_URL",
        "rpc_url": ARBITRUM_RPC_ENDPOINT,
        "tokens": ARBITRUM_TOKENS,
    },
    8453: {
        "name": "Base",
        "rpc_env": "STABLECOINS_BASE_RPC_URL",
        "rpc_url": BASE_RPC_ENDPOINT,
        "tokens": BASE_TOKENS,
    },
}
SOLANA_TOKENS = (
    {
        "symbol": "USDC",
        "name": "USD Coin",
        "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "decimals": 6,
    },
    {
        "symbol": "USDT",
        "name": "Tether USD",
        "address": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        "decimals": 6,
    },
)
TRON_API_URL = "https://api.trongrid.io"
TRON_TOKENS = (
    {
        "symbol": "USDT",
        "name": "Tether USD",
        "address": "TXLAQ63Xg1NAzckPwKHvzw7CSEmLMEqcdj",
        "decimals": 6,
    },
    {
        "symbol": "USDC",
        "name": "USD Coin (legacy)",
        "address": "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8",
        "decimals": 6,
    },
)

router = APIRouter(prefix="/stablecoins", tags=["stablecoins"])


def _normalize_evm_address(address: str) -> str:
    normalized = address.strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", normalized):
        raise HTTPException(
            status_code=400,
            detail="Address must be a 42-character EVM hex address",
        )
    return normalized.lower()


def _normalize_solana_wallet(wallet: str) -> str:
    normalized = wallet.strip()
    if not _is_solana_address(normalized):
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")
    return normalized


def _normalize_tron_wallet(wallet: str) -> str:
    normalized = wallet.strip()
    if not re.fullmatch(r"T[1-9A-HJ-NP-Za-km-z]{33}", normalized):
        raise HTTPException(status_code=400, detail="Invalid TRON wallet address")
    return normalized


def _format_balance(raw_amount: int, decimals: int) -> str:
    if raw_amount == 0:
        return "0"
    return format(Decimal(raw_amount) / (Decimal(10) ** decimals), "f")


def _evm_balance_id(chain_id: int, wallet: str, symbol: str) -> str:
    # Keep the original Ethereum identifier stable for formulas generated before
    # multi-chain EVM balances were introduced.
    namespace = "ethereum" if chain_id == 1 else "evm"
    return f"{namespace}:{chain_id}:{wallet}:{symbol}"


async def _fetch_evm_balances(
    client: httpx.AsyncClient, wallet: str, chain_id: int
) -> list[dict[str, str]]:
    chain = EVM_CHAINS.get(chain_id)
    if chain is None:
        supported = ", ".join(str(value) for value in sorted(EVM_CHAINS))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported stablecoin chain. Supported chain IDs: {supported}",
        )
    rpc_url = os.getenv(chain["rpc_env"]) or chain["rpc_url"]
    tokens = chain["tokens"]
    encoded_wallet = wallet[2:].lower().rjust(64, "0")
    payload = [
        {
            "jsonrpc": "2.0",
            "id": index,
            "method": "eth_call",
            "params": [
                {
                    "to": token["address"],
                    "data": f"0x{BALANCE_OF_SELECTOR}{encoded_wallet}",
                },
                "latest",
            ],
        }
        for index, token in enumerate(tokens)
    ]
    try:
        response = await client.post(rpc_url, json=payload)
        response.raise_for_status()
        items = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail=f"{chain['name']} RPC request failed"
        ) from exc

    if not isinstance(items, list):
        raise HTTPException(
            status_code=502,
            detail=f"{chain['name']} RPC returned an invalid batch response",
        )
    by_id = {
        item.get("id"): item
        for item in items
        if isinstance(item, dict) and isinstance(item.get("id"), int)
    }

    rows = []
    for index, token in enumerate(tokens):
        item = by_id.get(index, {})
        result = item.get("result") if isinstance(item, dict) else None
        try:
            raw_amount = int(result, 16) if isinstance(result, str) else None
        except ValueError:
            raw_amount = None
        if raw_amount is None:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to read {chain['name']} {token['symbol']} balance",
            )
        rows.append(
            {
                "balance_id": _evm_balance_id(
                    chain_id, wallet, token["symbol"]
                ),
                "wallet": wallet,
                "network": chain["name"],
                "chain_id": str(chain_id),
                "token_symbol": token["symbol"],
                "token_name": token["name"],
                "token_address": token["address"],
                "balance": _format_balance(raw_amount, token["decimals"]),
                "decimals": str(token["decimals"]),
            }
        )
    return rows


async def _fetch_ethereum_balances(
    client: httpx.AsyncClient, wallet: str
) -> list[dict[str, str]]:
    return await _fetch_evm_balances(client, wallet, 1)


def _sum_solana_token_accounts(result: Any, expected_decimals: int) -> int:
    values = result.get("value") if isinstance(result, dict) else None
    if not isinstance(values, list):
        raise HTTPException(
            status_code=502, detail="Solana RPC returned invalid token accounts"
        )

    total = 0
    for item in values:
        account = item.get("account") if isinstance(item, dict) else None
        data = account.get("data") if isinstance(account, dict) else None
        parsed = data.get("parsed") if isinstance(data, dict) else None
        info = parsed.get("info") if isinstance(parsed, dict) else None
        token_amount = info.get("tokenAmount") if isinstance(info, dict) else None
        if not isinstance(token_amount, dict):
            continue
        amount = token_amount.get("amount")
        decimals = token_amount.get("decimals")
        if not isinstance(amount, str) or decimals != expected_decimals:
            continue
        try:
            total += int(amount)
        except ValueError:
            continue
    return total


async def _fetch_solana_token_balance(
    client: httpx.AsyncClient,
    wallet: str,
    token: dict[str, Any],
    rpc_url: str,
) -> dict[str, str]:
    result = await _solana_rpc_request(
        client,
        "getTokenAccountsByOwner",
        [
            wallet,
            {"mint": token["address"]},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
        endpoint=rpc_url,
    )
    raw_amount = _sum_solana_token_accounts(result, token["decimals"])
    return {
        "balance_id": f"solana:mainnet:{wallet}:{token['symbol']}",
        "wallet": wallet,
        "network": "Solana",
        "chain_id": "solana-mainnet",
        "token_symbol": token["symbol"],
        "token_name": token["name"],
        "token_address": token["address"],
        "balance": _format_balance(raw_amount, token["decimals"]),
        "decimals": str(token["decimals"]),
    }


async def _fetch_solana_balances(
    client: httpx.AsyncClient, wallet: str
) -> list[dict[str, str]]:
    rpc_url = os.getenv("STABLECOINS_SOLANA_RPC_URL") or SOLANA_RPC_ENDPOINT
    return list(
        await asyncio.gather(
            *(
                _fetch_solana_token_balance(client, wallet, token, rpc_url)
                for token in SOLANA_TOKENS
            )
        )
    )


async def _fetch_tron_balances(
    client: httpx.AsyncClient, wallet: str
) -> list[dict[str, str]]:
    try:
        response = await client.get(f"{TRON_API_URL}/v1/accounts/{wallet}")
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(status_code=502, detail="TronGrid request failed") from exc
    data = payload.get("data") if isinstance(payload, dict) else None
    account = data[0] if isinstance(data, list) and data else {}
    balances = account.get("trc20", []) if isinstance(account, dict) else []
    if not isinstance(balances, list):
        raise HTTPException(status_code=502, detail="TronGrid returned invalid data")
    raw_by_contract = {}
    for item in balances:
        if not isinstance(item, dict):
            continue
        for contract, raw in item.items():
            try:
                raw_by_contract[contract] = raw_by_contract.get(contract, 0) + int(raw)
            except (TypeError, ValueError):
                continue
    return [
        {
            "balance_id": f"tron:mainnet:{wallet}:{token['symbol']}",
            "wallet": wallet,
            "network": "TRON",
            "chain_id": "tron-mainnet",
            "token_symbol": token["symbol"],
            "token_name": token["name"],
            "token_address": token["address"],
            "balance": _format_balance(
                raw_by_contract.get(token["address"], 0), token["decimals"]
            ),
            "decimals": str(token["decimals"]),
        }
        for token in TRON_TOKENS
    ]


async def _fetch_stablecoin_rows(
    address: str | None,
    wallet: str | None,
    chain_id: int = 1,
    tron_address: str | None = None,
) -> list[dict[str, str]]:
    normalized_address = _normalize_evm_address(address) if address else None
    normalized_wallet = _normalize_solana_wallet(wallet) if wallet else None
    normalized_tron = _normalize_tron_wallet(tron_address) if tron_address else None
    if (
        normalized_address is None
        and normalized_wallet is None
        and normalized_tron is None
    ):
        raise HTTPException(
            status_code=400,
            detail="Provide at least one EVM, Solana, or TRON wallet address",
        )

    async with queued_async_client(timeout=20.0, trust_env=False) as client:
        tasks = []
        if normalized_address:
            tasks.append(
                _fetch_evm_balances(client, normalized_address, chain_id)
            )
        if normalized_wallet:
            tasks.append(_fetch_solana_balances(client, normalized_wallet))
        if normalized_tron:
            tasks.append(_fetch_tron_balances(client, normalized_tron))
        groups = await asyncio.gather(*tasks)
    return [row for group in groups for row in group]


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=STABLECOIN_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/balances.csv",
    summary="Export EVM and Solana USDC/USDT balances",
    description=(
        "Returns stable USDC and USDT balance rows for an EVM address, "
        "a Solana wallet, a TRON wallet, or any combination. Zero balances "
        "are included."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_stablecoin_balances_csv(
    address: str | None = Query(None, description="Ethereum wallet address."),
    wallet: str | None = Query(None, description="Solana wallet address."),
    chain_id: int = Query(1, description="EVM chain ID: 1, 8453, or 42161."),
    tron_address: str | None = Query(None, description="TRON wallet address."),
):
    rows = await _fetch_stablecoin_rows(address, wallet, chain_id, tron_address)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={
            "Cache-Control": f"public, max-age={STABLECOINS_CACHE_TTL_SECONDS}"
        },
    )
