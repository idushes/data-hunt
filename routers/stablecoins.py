import asyncio
import csv
import io
import os
import re
from decimal import Decimal
from collections.abc import Callable
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from outbound_queue import queued_async_client

from routers.solana import (
    SPL_TOKEN_2022_PROGRAM_ID,
    SPL_TOKEN_PROGRAM_ID,
    _is_solana_address,
    _solana_rpc_request,
)


STABLECOINS_CACHE_TTL_SECONDS = 60
MAX_WALLETS_PER_REQUEST = 20
ETHEREUM_RPC_ENDPOINT = "https://ethereum-rpc.publicnode.com"
ARBITRUM_RPC_ENDPOINT = "https://arbitrum-one-rpc.publicnode.com"
BASE_RPC_ENDPOINT = "https://base-rpc.publicnode.com"
MONAD_RPC_ENDPOINT = "https://rpc.monad.xyz"
STABLECOINS_SOLANA_RPC_ENDPOINT = "https://api.mainnet-beta.solana.com"
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


def _token(
    symbol: str, name: str, address: str, decimals: int
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "name": name,
        "address": address,
        "decimals": decimals,
    }


# USDC and USDT stay first to preserve legacy row-based formulas. Additional
# USD stablecoins are a fixed, per-network snapshot of 24-hour market volume
# published on 2026-08-11. Symbols are unique because they form balance IDs.
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
    _token("USD1", "USD1", "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", 18),
    _token("USDG", "Global Dollar", "0xe343167631d89b6ffc58b88d6b7fb0228795491d", 6),
    _token("USDS", "USDS", "0xdc035d45d973e3ec169d2276ddab16f1e407384f", 18),
    _token("PYUSD", "PayPal USD", "0x6c3ea9036406852006290770bedfcaba0e23a0e8", 6),
    _token("USDD", "USDD", "0x4f8e5de400de08b164e7421b3ee387f461becd1a", 18),
    _token("DAI", "Dai", "0x6b175474e89094c44da98b954eedeac495271d0f", 18),
    _token("FDUSD", "First Digital USD", "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409", 18),
    _token("U", "United Stables", "0xce24439f2d9c6a2289f741120fe202248b666666", 18),
    _token("RLUSD", "Ripple USD", "0x8292bb45bf1ee4d140127049757c2e0ff06317ed", 18),
    _token("USDE", "Ethena USDe", "0x4c9edd5852cd905f086c759e8383e09bff1e68b3", 18),
    _token("TUSD", "TrueUSD", "0x0000000000085d4780b73119b644ae5ecd22b376", 18),
    _token("USDP", "Pax Dollar", "0x8e870d67f660d95d5be530380d0ec0bd388289e1", 18),
    _token("REUSD", "Re Protocol reUSD", "0x5086bf358635b81d8c47c66d1c8b9e567db70c72", 18),
    _token("C1USD", "Currency One USD", "0x40caa7912437002ee2c8415d43e7f575c733674c", 18),
    _token("XUSD", "StraitsX XUSD", "0xc08e7e23c235073c6807c2efe7021304cb7c2815", 6),
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
    _token("USDS", "USDS", "0x6491c05a82219b8d1479057361ff1654749b876b", 18),
    _token("PYUSD", "PayPal USD", "0x46850ad61c2b7d64d08c9c754f45254596696984", 6),
    _token("FDUSD", "First Digital USD", "0x93c9932e4afa59201f0b5e63f7d816516f1669fe", 18),
    _token("USDE", "Ethena USDe", "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34", 18),
    _token("REUSD", "Re Protocol reUSD", "0x76ce01f0ef25aa66cc5f1e546a005e4a63b25609", 18),
    _token("CRVUSD", "crvUSD", "0x498bf2b1e120fed3ad3d42ea2165e9b73f99c1e5", 18),
    _token("AUSD", "AUSD", "0x00000000efe302beaa2b3e6e1b18d08d69a9012a", 6),
    _token("FRXUSD", "Frax USD", "0x80eede496655fb9047dd39d9f418d5483ed600df", 18),
    _token("GHO", "GHO", "0x7dff72693f6a4149b17e7c6314655f6a9f7c8b33", 18),
    _token("EUSD", "Electronic USD", "0x12275dcb9048680c4be40942ea4d92c74c63b844", 18),
    _token("DOLA", "DOLA", "0x6a7661795c374c0bfc635934efaddff3a7ee23b6", 18),
    _token("FRAX", "Legacy Frax Dollar", "0x17fc002b466eec40dae837fc4be5c67993ddbd6f", 18),
    _token("USD0", "Usual USD", "0x35f1c5cb7fb977e669fd244c567da99d8a3a6850", 18),
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
    _token("USDS", "USDS", "0x820c137fa70c8691f0e44dc420a5e53c168921dc", 18),
    _token("USDE", "Ethena USDe", "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34", 18),
    _token("REUSD", "Re Protocol reUSD", "0x7d214438d0f27afccc23b3d1e1a53906ace5cfea", 18),
    _token("CRVUSD", "crvUSD", "0x417ac0e078398c154edfadd9ef675d30be60af93", 18),
    _token("AUSD", "AUSD", "0x00000000efe302beaa2b3e6e1b18d08d69a9012a", 6),
    _token("APXUSD", "apxUSD", "0xd993935e13851dd7517af10687ec7e5022127228", 18),
    _token("FXUSD", "f(x) Protocol fxUSD", "0x55380fe7a1910dff29a47b622057ab4139da42c5", 18),
    _token("FRXUSD", "Frax USD", "0xe5020a6d073a794b6e7f05678707de47986fb0b6", 18),
    _token("GHO", "GHO", "0x6bb7a212910682dcfdbd5bcbb3e28fb4e8da10ee", 18),
    _token("EUSD", "Electronic USD", "0xcfa3ef56d303ae4faaba0592388f19d7c3399fb4", 18),
    _token("DOLA", "DOLA", "0x4621b7a9c75199271f773ebd9a499dbd165c3191", 18),
    _token("MUSD", "Mezo USD", "0xdd468a1ddc392dcdbef6db6e34e89aa338f9f186", 18),
    _token("USD0", "Usual USD", "0x758a3e0b1f842c9306b783f8a4078c6c8c03a270", 18),
    _token("BOLD", "BOLD", "0x03569cc076654f82679c4ba2124d64774781b01d", 18),
)
MONAD_TOKENS = (
    _token("USDC", "USD Coin", "0x754704bc059f8c67012fed69bc8a327a5aafb603", 6),
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
    143: {
        "name": "Monad",
        "rpc_env": "STABLECOINS_MONAD_RPC_URL",
        "rpc_url": MONAD_RPC_ENDPOINT,
        "tokens": MONAD_TOKENS,
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
    _token("USD1", "USD1", "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB", 6),
    _token("USDG", "Global Dollar", "2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWjGWH", 6),
    _token("USDS", "USDS", "USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA", 6),
    _token("PYUSD", "PayPal USD", "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo", 6),
    _token("FDUSD", "First Digital USD", "9zNQRsGLjNKwCUU5Gq5LR8beUCPzQMVMqKAi3SSZh54u", 6),
    _token("USDE", "Ethena USDe", "DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT", 9),
    _token("USDGO", "USDGO", "72puLt71H93Z9CzHuBRTwFpL4TG3WZUhnoCC7p8gxigu", 6),
    _token("JUPUSD", "JupUSD", "JuprjznTrTSp2UFa3ZBUFgwdAmtZCq4MQCwysN55USD", 6),
    _token("USDP", "Pax Dollar", "HVbpJAQGNpkgBaYBZQBR1t7yFdvaYVp2vCQQfKKEN4tM", 6),
    _token("REUSD", "Re Protocol reUSD", "2uxaYT1fVrp6Fg2BrxQcyKSW91hefM6dG9krpbeDiirT", 9),
    _token("XUSD", "StraitsX XUSD", "4UbvZiomFvXDnZSz6vdHiDNiHozH2ykTEqjhhbVHiv9z", 6),
    _token("USDCV", "USD CoinVertible", "8smindLdDuySY6i2bStQX9o8DVhALCXCMbNxD98unx35", 2),
    _token("CASH", "CASH", "CASHx9KJUStyftLFWGvEVf59SGeG9sh5FfcnZMVPCASH", 6),
    _token("AUSD", "AUSD", "AUSD1jCcCyPLybk1YnvPWsHQSrZ46dxwoMniN4N2UEB9", 6),
    _token("SOFID", "SoFiUSD", "APhcqtzE73es3KAGiVksZFMLGwJDiAey5qZKUrQHEHfS", 6),
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
    _token("USD1", "USD1", "TPFqcBAaaUMCSVRCqPaQ9QnzKhmuoLR6Rc", 18),
    _token("USDD", "USDD", "TXDk8mbtRbXeYuMNS83CfKPaYYT8XWv9Hz", 18),
    _token("U", "United Stables", "TFNirp6PbqYE1ZTtWuCMUKJWLNZkoCoeFJ", 18),
    _token("TUSD", "TrueUSD", "TUpMhErZL2fhh4sVNULAbNKLokS4GjC1F4", 18),
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


def _split_wallets(
    value: str | None,
    normalizer: Callable[[str], str],
    wallet_type: str,
) -> list[str]:
    if not value:
        return []

    wallets: list[str] = []
    seen: set[str] = set()
    for candidate in re.split(r"[,;\n]+", value):
        if not candidate.strip():
            continue
        normalized = normalizer(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        wallets.append(normalized)

    if len(wallets) > MAX_WALLETS_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Too many {wallet_type} wallets. "
                f"Maximum: {MAX_WALLETS_PER_REQUEST}"
            ),
        )
    return wallets


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


def _sum_solana_balances_by_mint(
    results: tuple[Any, ...], tokens: tuple[dict[str, Any], ...]
) -> dict[str, int]:
    decimals_by_mint = {
        token["address"]: token["decimals"] for token in tokens
    }
    totals = {mint: 0 for mint in decimals_by_mint}
    for result in results:
        values = result.get("value") if isinstance(result, dict) else None
        if not isinstance(values, list):
            raise HTTPException(
                status_code=502,
                detail="Solana RPC returned invalid token accounts",
            )
        for item in values:
            account = item.get("account") if isinstance(item, dict) else None
            data = account.get("data") if isinstance(account, dict) else None
            parsed = data.get("parsed") if isinstance(data, dict) else None
            info = parsed.get("info") if isinstance(parsed, dict) else None
            token_amount = info.get("tokenAmount") if isinstance(info, dict) else None
            mint = info.get("mint") if isinstance(info, dict) else None
            if mint not in decimals_by_mint or not isinstance(token_amount, dict):
                continue
            amount = token_amount.get("amount")
            decimals = token_amount.get("decimals")
            if not isinstance(amount, str) or decimals != decimals_by_mint[mint]:
                continue
            try:
                totals[mint] += int(amount)
            except ValueError:
                continue
    return totals


async def _fetch_solana_balances(
    client: httpx.AsyncClient, wallet: str
) -> list[dict[str, str]]:
    rpc_url = (
        os.getenv("STABLECOINS_SOLANA_RPC_URL")
        or STABLECOINS_SOLANA_RPC_ENDPOINT
    )
    token_result, token_2022_result = await asyncio.gather(
        _solana_rpc_request(
            client,
            "getTokenAccountsByOwner",
            [
                wallet,
                {"programId": SPL_TOKEN_PROGRAM_ID},
                {"encoding": "jsonParsed", "commitment": "confirmed"},
            ],
            endpoint=rpc_url,
        ),
        _solana_rpc_request(
            client,
            "getTokenAccountsByOwner",
            [
                wallet,
                {"programId": SPL_TOKEN_2022_PROGRAM_ID},
                {"encoding": "jsonParsed", "commitment": "confirmed"},
            ],
            endpoint=rpc_url,
        ),
    )
    amounts_by_mint = _sum_solana_balances_by_mint(
        (token_result, token_2022_result), SOLANA_TOKENS
    )
    return [
        {
            "balance_id": f"solana:mainnet:{wallet}:{token['symbol']}",
            "wallet": wallet,
            "network": "Solana",
            "chain_id": "solana-mainnet",
            "token_symbol": token["symbol"],
            "token_name": token["name"],
            "token_address": token["address"],
            "balance": _format_balance(
                amounts_by_mint[token["address"]], token["decimals"]
            ),
            "decimals": str(token["decimals"]),
        }
        for token in SOLANA_TOKENS
    ]


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
    evm_wallets = _split_wallets(address, _normalize_evm_address, "EVM")
    solana_wallets = _split_wallets(wallet, _normalize_solana_wallet, "Solana")
    tron_wallets = _split_wallets(tron_address, _normalize_tron_wallet, "TRON")
    wallet_count = len(evm_wallets) + len(solana_wallets) + len(tron_wallets)
    if wallet_count > MAX_WALLETS_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Too many wallets. Maximum total: {MAX_WALLETS_PER_REQUEST}",
        )
    if not evm_wallets and not solana_wallets and not tron_wallets:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one EVM, Solana, or TRON wallet address",
        )

    async with queued_async_client(timeout=20.0, trust_env=False) as client:
        tasks = [
            *(
                _fetch_evm_balances(client, evm_wallet, chain_id)
                for evm_wallet in evm_wallets
            ),
            *(
                _fetch_solana_balances(client, solana_wallet)
                for solana_wallet in solana_wallets
            ),
            *(
                _fetch_tron_balances(client, tron_wallet)
                for tron_wallet in tron_wallets
            ),
        ]
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
    summary="Export popular EVM, Solana, and TRON stablecoin balances",
    description=(
        "Returns USDC, USDT, and up to 15 additional high-volume USD "
        "stablecoin balance rows available on each network. Pass multiple "
        "wallets as comma-separated values. Zero balances are included."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_stablecoin_balances_csv(
    address: str | None = Query(
        None, description="Comma-separated EVM wallet addresses."
    ),
    wallet: str | None = Query(
        None, description="Comma-separated Solana wallet addresses."
    ),
    chain_id: int = Query(
        1, description="EVM chain ID: 1, 143, 8453, or 42161."
    ),
    tron_address: str | None = Query(
        None, description="Comma-separated TRON wallet addresses."
    ),
):
    rows = await _fetch_stablecoin_rows(address, wallet, chain_id, tron_address)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={
            "Cache-Control": f"public, max-age={STABLECOINS_CACHE_TTL_SECONDS}"
        },
    )
