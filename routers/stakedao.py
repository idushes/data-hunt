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
STAKEDAO_APP_URL = "https://www.stakedao.org/strategy"
BLOCKSCOUT_API_URL = "https://eth.blockscout.com/api/v2"
CURVE_POOLS_API_URL = (
    "https://api.curve.finance/v1/getPools/ethereum/factory-stable-ng"
)
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
ASSET_SELECTOR = "38d52e0f"
TOTAL_ASSETS_SELECTOR = "01e1d114"
TOTAL_SUPPLY_SELECTOR = "18160ddd"
ACCOUNTANT_SELECTOR = "8b9d2940"
REWARD_TOKEN_SELECTOR = "99248ea7"
ACCOUNTS_SELECTOR = "ad74b775"
VAULTS_SELECTOR = "a622ee7c"
REWARD_SCALING_FACTOR = 10**27
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
    "claimable_reward_symbol",
    "claimable_reward_address",
    "claimable_reward_amount",
    "claimable_reward_price_usd",
    "claimable_reward_value_usd",
    "apr_current_percent",
    "apr_projected_percent",
    "apr_min_percent",
    "apr_max_percent",
    "tvl_usd",
    "source_url",
    "underlying_tvl_usd",
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


async def _fetch_list(client: httpx.AsyncClient, url: str) -> list[object]:
    try:
        response = await client.get(url)
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Stake DAO position discovery failed"
        ) from exc
    if not isinstance(payload, list):
        raise HTTPException(
            status_code=502, detail="Stake DAO position discovery returned invalid data"
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


def _v2_vault_candidates(payload: list[object]) -> list[dict[str, Any]]:
    candidates = []
    for value in payload:
        balance = _dict(value)
        token = _dict(balance.get("token"))
        name = _text(token.get("name"))
        symbol = _text(token.get("symbol"))
        address = _text(token.get("address_hash")).lower()
        raw_balance = _decimal(balance.get("value"))
        if (
            raw_balance is None
            or raw_balance <= 0
            or not re.fullmatch(r"0x[0-9a-f]{40}", address)
            or not name.lower().startswith("stake dao ")
            or not symbol.lower().startswith("sd-")
            or not symbol.lower().endswith("-vault")
        ):
            continue
        candidates.append(
            {
                "position_contract": address,
                "name": name.removeprefix("Stake DAO ").removesuffix(" Vault"),
                "vault_symbol": symbol,
            }
        )
        if len(candidates) >= 25:
            break
    return candidates


def _rpc_result_by_id(payload: object) -> dict[int, str]:
    if not isinstance(payload, list):
        return {}
    return {
        item["id"]: item["result"]
        for item in payload
        if isinstance(item, dict)
        and isinstance(item.get("id"), int)
        and isinstance(item.get("result"), str)
        and item["result"] != "0x"
    }


def _uint_words(value: str) -> list[int]:
    encoded = value.removeprefix("0x")
    if not encoded or len(encoded) % 64 != 0:
        return []
    try:
        return [
            int(encoded[offset : offset + 64], 16)
            for offset in range(0, len(encoded), 64)
        ]
    except ValueError:
        return []


def _reward_tokens_by_address(
    strategy_payloads: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    tokens: dict[str, dict[str, Any]] = {}
    for payload in strategy_payloads:
        deployed = payload.get("deployed")
        if not isinstance(deployed, list):
            continue
        for value in deployed:
            rewards = _dict(value).get("rewards")
            if not isinstance(rewards, list):
                continue
            for reward_value in rewards:
                reward = _dict(reward_value)
                token = _dict(reward.get("token"))
                address = _text(token.get("address")).lower()
                if not re.fullmatch(r"0x[0-9a-f]{40}", address):
                    continue
                current = tokens.setdefault(address, {})
                current.update(
                    {
                        "symbol": _text(token.get("symbol"))
                        or _text(current.get("symbol")),
                        "decimals": int(
                            token.get("decimals") or current.get("decimals") or 18
                        ),
                        "price_usd": reward.get("price")
                        if reward.get("price") is not None
                        else current.get("price_usd"),
                    }
                )
    return tokens


async def _fetch_v2_vault_states(
    client: httpx.AsyncClient,
    rpc_url: str,
    wallet: str,
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    encoded_wallet = wallet[2:].rjust(64, "0")
    calls = []
    selectors = (
        f"{BALANCE_OF_SELECTOR}{encoded_wallet}",
        ASSET_SELECTOR,
        TOTAL_ASSETS_SELECTOR,
        TOTAL_SUPPLY_SELECTOR,
    )
    for index, candidate in enumerate(candidates):
        for offset, selector in enumerate(selectors):
            calls.append(
                {
                    "jsonrpc": "2.0",
                    "id": index * len(selectors) + offset,
                    "method": "eth_call",
                    "params": [
                        {
                            "to": candidate["position_contract"],
                            "data": f"0x{selector}",
                        },
                        "latest",
                    ],
                }
            )
    if not calls:
        return []

    try:
        response = await client.post(rpc_url, json=calls)
        response.raise_for_status()
        results = _rpc_result_by_id(response.json())
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Stake DAO V2 RPC request failed"
        ) from exc

    states = []
    for index, candidate in enumerate(candidates):
        base = index * len(selectors)
        try:
            shares = int(results[base], 16)
            asset_result = results[base + 1]
            total_assets = int(results[base + 2], 16)
            total_supply = int(results[base + 3], 16)
        except (KeyError, ValueError):
            continue
        asset_address = f"0x{asset_result[-40:]}".lower()
        if (
            shares <= 0
            or total_assets <= 0
            or total_supply <= 0
            or not re.fullmatch(r"0x[0-9a-f]{40}", asset_address)
        ):
            continue
        states.append(
            {
                **candidate,
                "asset_address": asset_address,
                "raw_balance": shares * total_assets // total_supply,
                "total_assets": total_assets,
            }
        )
    return states


async def _fetch_v2_claimables(
    client: httpx.AsyncClient,
    rpc_url: str,
    wallet: str,
    states: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    state_by_vault = {
        _text(state.get("position_contract")): state for state in states
    }
    valid_vaults = [
        vault
        for vault in state_by_vault
        if re.fullmatch(r"0x[0-9a-f]{40}", vault)
    ]
    accountant_payload = [
        {
            "jsonrpc": "2.0",
            "id": index,
            "method": "eth_call",
            "params": [{"to": vault, "data": f"0x{ACCOUNTANT_SELECTOR}"}, "latest"],
        }
        for index, vault in enumerate(valid_vaults)
    ]
    if not accountant_payload:
        return {}

    try:
        response = await client.post(rpc_url, json=accountant_payload)
        response.raise_for_status()
        accountant_results = _rpc_result_by_id(response.json())
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Stake DAO V2 accountant RPC request failed"
        ) from exc

    accountant_by_vault = {}
    for index, vault in enumerate(valid_vaults):
        result = accountant_results.get(index, "")
        accountant = f"0x{result[-40:]}".lower()
        if (
            re.fullmatch(r"0x[0-9a-f]{40}", accountant)
            and int(accountant, 16) != 0
        ):
            accountant_by_vault[vault] = accountant

    accountants = sorted(set(accountant_by_vault.values()))
    calls = []
    reward_call_ids: dict[str, int] = {}
    state_call_ids: dict[str, tuple[int, int]] = {}

    for accountant in accountants:
        reward_call_ids[accountant] = len(calls)
        calls.append((accountant, REWARD_TOKEN_SELECTOR))

    encoded_wallet = wallet[2:].rjust(64, "0")
    for state in states:
        vault = _text(state.get("position_contract"))
        accountant = accountant_by_vault.get(vault)
        if accountant is None:
            continue
        encoded_vault = vault[2:].rjust(64, "0")
        account_call_id = len(calls)
        calls.append(
            (
                accountant,
                f"{ACCOUNTS_SELECTOR}{encoded_vault}{encoded_wallet}",
            )
        )
        vault_call_id = len(calls)
        calls.append((accountant, f"{VAULTS_SELECTOR}{encoded_vault}"))
        state_call_ids[vault] = (account_call_id, vault_call_id)

    if not calls:
        return {}

    payload = [
        {
            "jsonrpc": "2.0",
            "id": index,
            "method": "eth_call",
            "params": [{"to": address, "data": f"0x{data}"}, "latest"],
        }
        for index, (address, data) in enumerate(calls)
    ]
    try:
        response = await client.post(rpc_url, json=payload)
        response.raise_for_status()
        results = _rpc_result_by_id(response.json())
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Stake DAO V2 rewards RPC request failed"
        ) from exc

    reward_by_accountant = {}
    for accountant, call_id in reward_call_ids.items():
        result = results.get(call_id, "")
        reward_address = f"0x{result[-40:]}".lower()
        if (
            re.fullmatch(r"0x[0-9a-f]{40}", reward_address)
            and int(reward_address, 16) != 0
        ):
            reward_by_accountant[accountant] = reward_address

    claimables = {}
    for vault, (account_call_id, vault_call_id) in state_call_ids.items():
        account_words = _uint_words(results.get(account_call_id, ""))
        vault_words = _uint_words(results.get(vault_call_id, ""))
        reward_address = reward_by_accountant.get(accountant_by_vault[vault])
        if len(account_words) < 3 or not vault_words or reward_address is None:
            continue
        account_balance, account_integral, pending_rewards = account_words[:3]
        vault_integral = vault_words[0]
        integral_rewards = (
            (vault_integral - account_integral)
            * account_balance
            // REWARD_SCALING_FACTOR
            if vault_integral > account_integral
            else 0
        )
        claimables[vault] = {
            "reward_address": reward_address,
            "raw_amount": pending_rewards + integral_rewards,
        }
    return claimables


def _curve_pools_by_lp(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    pools = _dict(payload.get("data")).get("poolData")
    if not isinstance(pools, list):
        return {}
    by_lp = {}
    for value in pools:
        pool = _dict(value)
        lp_token = (
            _text(pool.get("lpTokenAddress")) or _text(pool.get("address"))
        ).lower()
        if re.fullmatch(r"0x[0-9a-f]{40}", lp_token):
            by_lp[lp_token] = pool
    return by_lp


def _v2_vault_targets(
    states: list[dict[str, Any]],
    curve_payload: dict[str, Any],
    chain_id: int,
    claimables: dict[str, dict[str, Any]] | None = None,
    reward_tokens: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[int]]:
    pools = _curve_pools_by_lp(curve_payload)
    targets = []
    balances = []
    for state in states:
        asset_address = _text(state.get("asset_address"))
        pool = pools.get(asset_address)
        if pool is None:
            continue
        pool_decimals = pool.get("decimals")
        decimals = (
            int(pool_decimals)
            if isinstance(pool_decimals, (int, str))
            else 18
        )
        total_supply = _decimal(pool.get("totalSupply"))
        underlying_tvl = _decimal(pool.get("usdTotal"))
        if total_supply is None or total_supply <= 0 or underlying_tvl is None:
            continue
        lp_supply = total_supply / (Decimal(10) ** decimals)
        price = underlying_tvl / lp_supply
        gauge_apy = pool.get("gaugeCrvApy")
        apy_values = (
            [_decimal(value) for value in gauge_apy]
            if isinstance(gauge_apy, list)
            else []
        )
        apy_values = [value for value in apy_values if value is not None]
        vault_tvl = (
            Decimal(int(state["total_assets"]))
            / (Decimal(10) ** decimals)
            * price
        )
        contract = _text(state.get("position_contract"))
        target = {
            "product": "strategy",
            "protocol": "curve",
            "strategy_key": _text(pool.get("id")),
            "name": _text(pool.get("name")) or _text(state.get("name")),
            "position_type": "vault_v2",
            "position_contract": contract,
            "asset_symbol": _text(pool.get("symbol")),
            "asset_address": asset_address,
            "decimals": decimals,
            "price_usd": price,
            "apr_current_percent": "",
            "apr_projected_percent": max(apy_values, default=None),
            "apr_min_percent": min(apy_values, default=None),
            "apr_max_percent": max(apy_values, default=None),
            "tvl_usd": vault_tvl,
            "underlying_tvl_usd": underlying_tvl,
            "source_url": (
                f"{STAKEDAO_APP_URL}?protocol=curve&vault={chain_id}-{contract}"
            ),
        }
        claimable = (claimables or {}).get(contract)
        if claimable is not None:
            reward_address = _text(claimable.get("reward_address"))
            reward = (reward_tokens or {}).get(reward_address, {})
            reward_decimals = int(reward.get("decimals") or 18)
            reward_amount = Decimal(int(claimable.get("raw_amount") or 0)) / (
                Decimal(10) ** reward_decimals
            )
            reward_price = _decimal(reward.get("price_usd"))
            target.update(
                {
                    "claimable_reward_symbol": _text(reward.get("symbol")),
                    "claimable_reward_address": reward_address,
                    "claimable_reward_amount": reward_amount,
                    "claimable_reward_price_usd": reward_price,
                    "claimable_reward_value_usd": (
                        reward_amount * reward_price
                        if reward_price is not None
                        else None
                    ),
                }
            )
        targets.append(target)
        balances.append(int(state["raw_balance"]))
    return targets, balances


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
                "claimable_reward_symbol": _text(
                    target.get("claimable_reward_symbol")
                ),
                "claimable_reward_address": _text(
                    target.get("claimable_reward_address")
                ),
                "claimable_reward_amount": _format_decimal(
                    _decimal(target.get("claimable_reward_amount"))
                ),
                "claimable_reward_price_usd": _format_decimal(
                    _decimal(target.get("claimable_reward_price_usd"))
                ),
                "claimable_reward_value_usd": _format_decimal(
                    _decimal(target.get("claimable_reward_value_usd"))
                ),
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
                "underlying_tvl_usd": _format_decimal(
                    _decimal(target.get("underlying_tvl_usd"))
                ),
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
        strategy_payloads, lockers, token_balances = await asyncio.gather(
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
            _fetch_list(
                client,
                f"{BLOCKSCOUT_API_URL}/addresses/{wallet}/token-balances",
            ),
        )
        targets = []
        for protocol, payload in zip(
            STAKEDAO_STRATEGY_PROTOCOLS, strategy_payloads, strict=False
        ):
            targets.extend(_strategy_targets(payload, protocol, chain_id))
        targets.extend(_locker_targets(lockers, chain_id))

        rpc_url = os.getenv(str(chain["rpc_env"])) or str(chain["rpc_url"])
        balances, v2_states = await asyncio.gather(
            _fetch_balances(client, rpc_url, wallet, targets),
            _fetch_v2_vault_states(
                client,
                rpc_url,
                wallet,
                _v2_vault_candidates(token_balances),
            ),
        )
        if v2_states:
            curve_payload, v2_claimables = await asyncio.gather(
                _fetch_json(client, CURVE_POOLS_API_URL),
                _fetch_v2_claimables(client, rpc_url, wallet, v2_states),
            )
            existing_contracts = {
                _text(target.get("position_contract")) for target in targets
            }
            v2_states = [
                state
                for state in v2_states
                if _text(state.get("position_contract")) not in existing_contracts
            ]
            v2_targets, v2_balances = _v2_vault_targets(
                v2_states,
                curve_payload,
                chain_id,
                v2_claimables,
                _reward_tokens_by_address(list(strategy_payloads)),
            )
            targets.extend(v2_targets)
            balances.extend(v2_balances)

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
