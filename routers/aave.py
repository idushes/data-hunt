import asyncio
import base64
import csv
import io
import os
import re
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from eth_abi import decode, encode
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from web3 import Web3

from outbound_queue import queued_async_client


AAVE_V3_API_URL = "https://api.v3.aave.com/graphql"
AAVE_V4_API_URL = "https://api.v4.aave.com/graphql"
AAVE_V4_CHAIN_IDS = {1, 43114}
AAVE_V4_ETHEREUM_RPC_URL = "https://ethereum-rpc.publicnode.com"
AAVE_V4_ONCHAIN_SPOKES = {
    1: (
        ("Main", "0x94e7a5dcbe816e498b89ab752661904e2f56c485"),
    ),
}
AAVE_CACHE_TTL_SECONDS = 60
AAVE_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "market",
    "market_address",
    "position_id",
    "token_symbol",
    "token_name",
    "token_address",
    "token_price_usd",
    "supply_amount",
    "supply_usd",
    "supply_apy_percent",
    "is_collateral",
    "can_be_collateral",
    "borrow_amount",
    "borrow_usd",
    "borrow_apy_percent",
    "net_usd",
    "health_factor",
    "ltv_percent",
    "liquidation_threshold_percent",
    "protocol_version",
    "account_position_id",
    "supply_principal_amount",
    "supply_interest_amount",
    "borrow_principal_amount",
    "borrow_interest_amount",
    "position_total_supplied_usd",
    "position_total_collateral_usd",
    "position_total_debt_usd",
    "position_net_balance_usd",
    "position_net_apy_percent",
    "position_net_supply_apy_percent",
    "position_net_borrow_apy_percent",
    "position_net_accrued_interest_usd",
    "position_average_collateral_factor_percent",
]

MARKETS_QUERY = """
query AaveMarkets($request: MarketsRequest!) {
  value: markets(request: $request) {
    name
    address
    chain { name chainId }
    userState {
      healthFactor
      ltv { formatted }
      currentLiquidationThreshold { formatted }
    }
  }
}
"""

POSITIONS_QUERY = """
query AavePositions(
  $suppliesRequest: UserSuppliesRequest!
  $borrowsRequest: UserBorrowsRequest!
) {
  supplies: userSupplies(request: $suppliesRequest) {
    market { name address chain { name chainId } }
    currency { address name symbol chainId }
    balance { usdPerToken usd amount { value } }
    apy { formatted }
    isCollateral
    canBeCollateral
  }
  borrows: userBorrows(request: $borrowsRequest) {
    market { name address chain { name chainId } }
    currency { address name symbol chainId }
    debt { usdPerToken usd amount { value } }
    apy { formatted }
  }
}
"""

AAVE_V4_POSITIONS_QUERY = """
query AaveV4Positions(
  $positionsRequest: UserPositionsRequest!
  $suppliesRequest: UserSuppliesRequest!
  $borrowsRequest: UserBorrowsRequest!
) {
  positions: userPositions(request: $positionsRequest) {
    id
    spoke {
      id
      name
      address
      chain {
        name
        chainId
        nativeWrappedToken
        nativeInfo { name symbol }
      }
    }
    totalSupplied { current { value } }
    totalCollateral { current { value } }
    totalDebt { current { value } }
    netBalance { current { value } }
    netApy { normalized }
    netSupplyApy { current { normalized } }
    netBorrowApy { current { normalized } }
    netAccruedInterest { value }
    healthFactor { current }
    averageCollateralFactor { normalized }
  }
  supplies: userSupplies(request: $suppliesRequest) {
    id
    isCollateral
    principal { amount { value } }
    interest { amount { value } }
    balance {
      amount { value }
      exchange { value }
      exchangeRate { value }
      isWrappedNative
      token { address info { name symbol } }
    }
    reserve {
      canUseAsCollateral
      summary { supplyApy { normalized } }
      spoke { id }
    }
  }
  borrows: userBorrows(request: $borrowsRequest) {
    id
    principal { amount { value } }
    interest { amount { value } }
    debt {
      amount { value }
      exchange { value }
      exchangeRate { value }
      isWrappedNative
      token { address info { name symbol } }
    }
    reserve {
      summary { borrowApy { normalized } }
      spoke { id }
    }
  }
}
"""

router = APIRouter(prefix="/aave", tags=["aave"])


def _normalize_wallet(address: str) -> str:
    normalized = address.strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", normalized):
        raise HTTPException(
            status_code=400,
            detail="Address must be a 42-character EVM hex address",
        )
    return normalized.lower()


def _text(value: object | None) -> str:
    return "" if value is None else str(value)


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


def _dict(value: object | None) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _empty_row(
    wallet: str,
    chain_id: int,
    market: dict[str, Any],
    currency: dict[str, Any],
) -> dict[str, str]:
    chain = _dict(market.get("chain"))
    market_address = _text(market.get("address")).lower()
    token_address = _text(currency.get("address")).lower()
    row = {column: "" for column in AAVE_CSV_HEADER}
    row.update(
        {
            "wallet": wallet,
            "chain_id": str(chain_id),
            "chain": _text(chain.get("name")),
            "market": _text(market.get("name")),
            "market_address": market_address,
            "position_id": f"{market_address}:{token_address}",
            "token_symbol": _text(currency.get("symbol")),
            "token_name": _text(currency.get("name")),
            "token_address": token_address,
            "is_collateral": "false",
            "can_be_collateral": "false",
            "protocol_version": "v3",
        }
    )
    return row


def _market_health(markets: list[object]) -> dict[str, dict[str, str]]:
    health_by_market: dict[str, dict[str, str]] = {}
    for value in markets:
        market = _dict(value)
        market_address = _text(market.get("address")).lower()
        if not market_address:
            continue
        state = _dict(market.get("userState"))
        health_by_market[market_address] = {
            "health_factor": _text(state.get("healthFactor")),
            "ltv_percent": _text(_dict(state.get("ltv")).get("formatted")),
            "liquidation_threshold_percent": _text(
                _dict(state.get("currentLiquidationThreshold")).get("formatted")
            ),
        }
    return health_by_market


def _parse_positions(
    wallet: str,
    chain_id: int,
    markets: list[object],
    payload: object,
) -> list[dict[str, str]]:
    data = _dict(payload)
    health_by_market = _market_health(markets)
    rows_by_position: dict[str, dict[str, str]] = {}

    for side, amount_key in (("supplies", "balance"), ("borrows", "debt")):
        positions = data.get(side)
        if not isinstance(positions, list):
            continue
        for value in positions:
            position = _dict(value)
            market = _dict(position.get("market"))
            currency = _dict(position.get("currency"))
            market_address = _text(market.get("address")).lower()
            token_address = _text(currency.get("address")).lower()
            if not market_address or not token_address:
                continue

            position_id = f"{market_address}:{token_address}"
            row = rows_by_position.setdefault(
                position_id,
                _empty_row(wallet, chain_id, market, currency),
            )
            row.update(health_by_market.get(market_address, {}))

            amount = _dict(position.get(amount_key))
            apy = _dict(position.get("apy"))
            row["token_price_usd"] = _text(amount.get("usdPerToken"))
            if side == "supplies":
                row["supply_amount"] = _text(
                    _dict(amount.get("amount")).get("value")
                )
                row["supply_usd"] = _text(amount.get("usd"))
                row["supply_apy_percent"] = _text(apy.get("formatted"))
                row["is_collateral"] = str(
                    bool(position.get("isCollateral"))
                ).lower()
                row["can_be_collateral"] = str(
                    bool(position.get("canBeCollateral"))
                ).lower()
            else:
                row["borrow_amount"] = _text(
                    _dict(amount.get("amount")).get("value")
                )
                row["borrow_usd"] = _text(amount.get("usd"))
                row["borrow_apy_percent"] = _text(apy.get("formatted"))

    for row in rows_by_position.values():
        row["net_usd"] = _format_decimal(
            _decimal_or_zero(row.get("supply_usd"))
            - _decimal_or_zero(row.get("borrow_usd"))
        )

    return sorted(rows_by_position.values(), key=lambda row: row["position_id"])


def _graphql_error(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return "Aave API returned an invalid response"
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, dict) and isinstance(first.get("message"), str):
            return f"Aave API error: {first['message']}"
        return "Aave API returned a GraphQL error"
    if not isinstance(payload.get("data"), dict):
        return "Aave API response is missing data"
    return None


async def _fetch_graphql(
    client: httpx.AsyncClient,
    query: str,
    variables: dict[str, object],
    api_url: str = AAVE_V3_API_URL,
) -> dict[str, Any]:
    try:
        response = await client.post(
            api_url,
            json={"query": query, "variables": variables},
        )
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Aave API returned HTTP {exc.response.status_code}",
        ) from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            status_code=502,
            detail="Aave API request failed",
        ) from exc

    error = _graphql_error(payload)
    if error:
        raise HTTPException(status_code=502, detail=error)
    return payload["data"]


def _contract_call_data(
    signature: str,
    types: list[str] | None = None,
    values: list[object] | None = None,
) -> str:
    payload = Web3.keccak(text=signature)[:4]
    if types:
        payload += encode(types, values or [])
    return "0x" + payload.hex()


def _decode_contract_result(result: object, types: list[str]) -> tuple[Any, ...]:
    if not isinstance(result, str) or not result.startswith("0x"):
        raise ValueError("missing eth_call result")
    return decode(types, bytes.fromhex(result[2:]))


async def _aave_v4_rpc_batch(
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
        raise HTTPException(
            status_code=502,
            detail="Aave V4 on-chain fallback request failed",
        ) from exc

    if not isinstance(values, list):
        raise HTTPException(
            status_code=502,
            detail="Aave V4 on-chain fallback returned invalid data",
        )
    by_id = {
        item.get("id"): item
        for item in values
        if isinstance(item, dict) and isinstance(item.get("id"), int)
    }
    decoded: list[tuple[Any, ...]] = []
    for index, (_address, _data, output_types) in enumerate(calls):
        item = by_id.get(index, {})
        try:
            decoded.append(_decode_contract_result(item.get("result"), output_types))
        except (TypeError, ValueError) as exc:
            error = item.get("error") if isinstance(item, dict) else None
            detail = "Aave V4 on-chain fallback returned undecodable data"
            if isinstance(error, dict) and error.get("message"):
                detail = f"Aave V4 on-chain fallback RPC error: {error['message']}"
            raise HTTPException(
                status_code=502,
                detail=detail,
            ) from exc
    return decoded


async def _aave_v4_rpc_call(
    client: httpx.AsyncClient,
    rpc_url: str,
    address: str,
    signature: str,
    output_types: list[str],
) -> tuple[Any, ...]:
    values = await _aave_v4_rpc_batch(
        client,
        rpc_url,
        [(address, _contract_call_data(signature), output_types)],
    )
    return values[0]


def _position_account_id(chain_id: int, spoke: str, wallet: str) -> str:
    raw = (
        f"{chain_id}::{Web3.to_checksum_address(spoke)}::"
        f"{Web3.to_checksum_address(wallet)}"
    )
    return base64.b64encode(raw.encode()).decode()


async def _erc20_metadata(
    client: httpx.AsyncClient,
    rpc_url: str,
    token_addresses: list[str],
) -> dict[str, tuple[str, str]]:
    calls = [
        (token, _contract_call_data(signature), ["string"])
        for token in token_addresses
        for signature in ("name()", "symbol()")
    ]
    values = await _aave_v4_rpc_batch(client, rpc_url, calls)
    return {
        token.lower(): (_text(values[index * 2][0]), _text(values[index * 2 + 1][0]))
        for index, token in enumerate(token_addresses)
    }


async def _fetch_aave_v4_onchain_spoke_rows(
    client: httpx.AsyncClient,
    wallet: str,
    chain_id: int,
    market_name: str,
    spoke: str,
    rpc_url: str,
) -> list[dict[str, str]]:
    reserve_count = int(
        (await _aave_v4_rpc_call(client, rpc_url, spoke, "getReserveCount()", ["uint256"]))[0]
    )
    if reserve_count <= 0:
        return []

    reserve_types = ["address", "address", "uint16", "uint8", "uint24", "uint8", "uint32"]
    position_types = ["uint120", "uint120", "int200", "uint120", "uint32"]
    calls: list[tuple[str, str, list[str]]] = []
    for reserve_id in range(reserve_count):
        calls.extend(
            [
                (
                    spoke,
                    _contract_call_data("getReserve(uint256)", ["uint256"], [reserve_id]),
                    reserve_types,
                ),
                (
                    spoke,
                    _contract_call_data(
                        "getUserSuppliedAssets(uint256,address)",
                        ["uint256", "address"],
                        [reserve_id, wallet],
                    ),
                    ["uint256"],
                ),
                (
                    spoke,
                    _contract_call_data(
                        "getUserTotalDebt(uint256,address)",
                        ["uint256", "address"],
                        [reserve_id, wallet],
                    ),
                    ["uint256"],
                ),
                (
                    spoke,
                    _contract_call_data(
                        "getUserReserveStatus(uint256,address)",
                        ["uint256", "address"],
                        [reserve_id, wallet],
                    ),
                    ["bool", "bool"],
                ),
                (
                    spoke,
                    _contract_call_data(
                        "getUserPosition(uint256,address)",
                        ["uint256", "address"],
                        [reserve_id, wallet],
                    ),
                    position_types,
                ),
            ]
        )
    values = await _aave_v4_rpc_batch(client, rpc_url, calls)

    active: list[dict[str, Any]] = []
    for reserve_id in range(reserve_count):
        offset = reserve_id * 5
        reserve = values[offset]
        supplied = int(values[offset + 1][0])
        debt = int(values[offset + 2][0])
        collateral, borrowing = values[offset + 3]
        dynamic_config_key = int(values[offset + 4][4])
        if supplied == 0 and debt == 0:
            continue
        active.append(
            {
                "reserve_id": reserve_id,
                "token": _text(reserve[0]).lower(),
                "decimals": int(reserve[3]),
                "supplied": supplied,
                "debt": debt,
                "collateral": bool(collateral),
                "borrowing": bool(borrowing),
                "dynamic_config_key": dynamic_config_key,
            }
        )
    if not active:
        return []

    oracle = _text(
        (await _aave_v4_rpc_call(client, rpc_url, spoke, "ORACLE()", ["address"]))[0]
    )
    detail_calls: list[tuple[str, str, list[str]]] = []
    for item in active:
        detail_calls.extend(
            [
                (
                    oracle,
                    _contract_call_data(
                        "getReservePrice(uint256)",
                        ["uint256"],
                        [item["reserve_id"]],
                    ),
                    ["uint256"],
                ),
                (
                    spoke,
                    _contract_call_data(
                        "getDynamicReserveConfig(uint256,uint32)",
                        ["uint256", "uint32"],
                        [item["reserve_id"], item["dynamic_config_key"]],
                    ),
                    ["uint16", "uint32", "uint16"],
                ),
            ]
        )
    details = await _aave_v4_rpc_batch(client, rpc_url, detail_calls)
    metadata = await _erc20_metadata(
        client,
        rpc_url,
        [item["token"] for item in active],
    )
    try:
        account_data = await _aave_v4_rpc_call(
            client,
            rpc_url,
            spoke,
            "getUserAccountData(address)",
            ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
        )
    except HTTPException:
        await asyncio.sleep(0.25)
        account_data = await _aave_v4_rpc_call(
            client,
            rpc_url,
            spoke,
            "getUserAccountData(address)",
            ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
        )

    account_position_id = _position_account_id(chain_id, spoke, wallet)
    total_supplied_usd = Decimal("0")
    total_debt_usd = Decimal("0")
    rows: list[dict[str, str]] = []
    for index, item in enumerate(active):
        price = Decimal(int(details[index * 2][0])) / Decimal(10**8)
        collateral_factor = Decimal(int(details[index * 2 + 1][0])) / Decimal(100)
        scale = Decimal(10 ** item["decimals"])
        supplied = Decimal(item["supplied"]) / scale
        debt = Decimal(item["debt"]) / scale
        supplied_usd = supplied * price
        debt_usd = debt * price
        total_supplied_usd += supplied_usd
        total_debt_usd += debt_usd
        token_name, token_symbol = metadata.get(item["token"], ("", ""))
        if item["token"] == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2":
            token_name, token_symbol = "Ether", "ETH"

        row = {column: "" for column in AAVE_CSV_HEADER}
        row.update(
            {
                "wallet": wallet,
                "chain_id": str(chain_id),
                "chain": "Ethereum",
                "market": f"Aave V4 {market_name}",
                "market_address": spoke,
                "position_id": f"v4:{account_position_id}:{item['token']}",
                "token_symbol": token_symbol,
                "token_name": token_name,
                "token_address": item["token"],
                "token_price_usd": _format_decimal(price),
                "supply_amount": _format_decimal(supplied) if supplied else "",
                "supply_usd": _format_decimal(supplied_usd) if supplied else "",
                "is_collateral": str(item["collateral"]).lower(),
                "can_be_collateral": str(collateral_factor > 0).lower(),
                "borrow_amount": _format_decimal(debt) if debt else "",
                "borrow_usd": _format_decimal(debt_usd) if debt else "",
                "net_usd": _format_decimal(supplied_usd - debt_usd),
                "health_factor": (
                    _format_decimal(Decimal(int(account_data[2])) / Decimal(10**18))
                    if int(account_data[4]) > 0
                    else ""
                ),
                "protocol_version": "v4",
                "account_position_id": account_position_id,
                "position_total_collateral_usd": _format_decimal(
                    Decimal(int(account_data[3])) / Decimal(10**26)
                ),
                "position_average_collateral_factor_percent": _format_decimal(
                    Decimal(int(account_data[1])) / Decimal(10**16)
                ),
            }
        )
        rows.append(row)

    total_collateral_usd = Decimal(int(account_data[3])) / Decimal(10**26)
    net_usd = total_supplied_usd - total_debt_usd
    for row in rows:
        row["position_total_supplied_usd"] = _format_decimal(total_supplied_usd)
        row["position_total_debt_usd"] = _format_decimal(total_debt_usd)
        row["position_net_balance_usd"] = _format_decimal(net_usd)
        row["position_total_collateral_usd"] = _format_decimal(total_collateral_usd)
    return sorted(rows, key=lambda row: row["position_id"])


async def _fetch_aave_v4_onchain_rows(
    client: httpx.AsyncClient,
    wallet: str,
    chain_id: int,
) -> list[dict[str, str]]:
    spokes = AAVE_V4_ONCHAIN_SPOKES.get(chain_id)
    if not spokes:
        raise HTTPException(
            status_code=502,
            detail="Aave V4 on-chain fallback is unavailable for this chain",
        )
    rpc_url = os.getenv("AAVE_V4_ETHEREUM_RPC_URL") or AAVE_V4_ETHEREUM_RPC_URL
    results = await asyncio.gather(
        *(
            _fetch_aave_v4_onchain_spoke_rows(
                client,
                wallet,
                chain_id,
                market_name,
                spoke,
                rpc_url,
            )
            for market_name, spoke in spokes
        )
    )
    return sorted(
        (row for spoke_rows in results for row in spoke_rows),
        key=lambda row: row["position_id"],
    )


async def _fetch_aave_v3_rows(
    client: httpx.AsyncClient,
    wallet: str,
    chain_id: int,
) -> list[dict[str, str]]:
    market_data = await _fetch_graphql(
        client,
        MARKETS_QUERY,
        {"request": {"chainIds": [chain_id], "user": wallet}},
    )
    markets = market_data.get("value")
    if not isinstance(markets, list) or not markets:
        raise HTTPException(
            status_code=400,
            detail=f"Aave has no markets for chain ID {chain_id}",
        )

    market_inputs = [
        {"address": address, "chainId": chain_id}
        for market in markets
        if isinstance(market, dict)
        and (address := _text(market.get("address")))
    ]
    position_request = {"markets": market_inputs, "user": wallet}
    position_data = await _fetch_graphql(
        client,
        POSITIONS_QUERY,
        {
            "suppliesRequest": position_request,
            "borrowsRequest": position_request,
        },
    )
    return _parse_positions(wallet, chain_id, markets, position_data)


def _nested_text(value: object | None, *keys: str) -> str:
    current = value
    for key in keys:
        current = _dict(current).get(key)
    return _text(current)


def _v4_position_metadata(position: dict[str, Any]) -> dict[str, str]:
    spoke = _dict(position.get("spoke"))
    chain = _dict(spoke.get("chain"))
    return {
        "account_position_id": _text(position.get("id")),
        "chain": _text(chain.get("name")),
        "market": f"Aave V4 {_text(spoke.get('name'))}".strip(),
        "market_address": _text(spoke.get("address")).lower(),
        "health_factor": _nested_text(position, "healthFactor", "current"),
        "position_total_supplied_usd": _nested_text(
            position, "totalSupplied", "current", "value"
        ),
        "position_total_collateral_usd": _nested_text(
            position, "totalCollateral", "current", "value"
        ),
        "position_total_debt_usd": _nested_text(
            position, "totalDebt", "current", "value"
        ),
        "position_net_balance_usd": _nested_text(
            position, "netBalance", "current", "value"
        ),
        "position_net_apy_percent": _nested_text(position, "netApy", "normalized"),
        "position_net_supply_apy_percent": _nested_text(
            position, "netSupplyApy", "current", "normalized"
        ),
        "position_net_borrow_apy_percent": _nested_text(
            position, "netBorrowApy", "current", "normalized"
        ),
        "position_net_accrued_interest_usd": _nested_text(
            position, "netAccruedInterest", "value"
        ),
        "position_average_collateral_factor_percent": _nested_text(
            position, "averageCollateralFactor", "normalized"
        ),
    }


def _v4_empty_row(
    wallet: str,
    chain_id: int,
    position: dict[str, Any],
    amount: dict[str, Any],
) -> dict[str, str]:
    metadata = _v4_position_metadata(position)
    token = _dict(amount.get("token"))
    token_info = _dict(token.get("info"))
    chain = _dict(_dict(position.get("spoke")).get("chain"))
    token_address = _text(token.get("address")).lower()
    is_native_wrapped_token = token_address == _text(
        chain.get("nativeWrappedToken")
    ).lower()
    if amount.get("isWrappedNative") or is_native_wrapped_token:
        native_info = _dict(chain.get("nativeInfo"))
        if native_info:
            token_info = native_info

    account_position_id = metadata["account_position_id"]
    row = {column: "" for column in AAVE_CSV_HEADER}
    row.update(metadata)
    row.update(
        {
            "wallet": wallet,
            "chain_id": str(chain_id),
            "position_id": f"v4:{account_position_id}:{token_address}",
            "token_symbol": _text(token_info.get("symbol")),
            "token_name": _text(token_info.get("name")),
            "token_address": token_address,
            "is_collateral": "false",
            "can_be_collateral": "false",
            "protocol_version": "v4",
        }
    )
    return row


def _parse_v4_positions(
    wallet: str,
    chain_id: int,
    payload: object,
) -> list[dict[str, str]]:
    data = _dict(payload)
    positions = data.get("positions")
    position_by_spoke: dict[str, dict[str, Any]] = {}
    if isinstance(positions, list):
        for value in positions:
            position = _dict(value)
            spoke_id = _nested_text(position, "spoke", "id")
            if spoke_id:
                position_by_spoke[spoke_id] = position

    rows_by_position: dict[str, dict[str, str]] = {}
    for side, amount_key in (("supplies", "balance"), ("borrows", "debt")):
        items = data.get(side)
        if not isinstance(items, list):
            continue
        for value in items:
            item = _dict(value)
            reserve = _dict(item.get("reserve"))
            position = position_by_spoke.get(_nested_text(reserve, "spoke", "id"))
            amount = _dict(item.get(amount_key))
            token_address = _nested_text(amount, "token", "address").lower()
            if position is None or not token_address:
                continue

            account_position_id = _text(position.get("id"))
            position_id = f"v4:{account_position_id}:{token_address}"
            row = rows_by_position.setdefault(
                position_id,
                _v4_empty_row(wallet, chain_id, position, amount),
            )
            row["token_price_usd"] = _nested_text(amount, "exchangeRate", "value")
            if side == "supplies":
                row["supply_amount"] = _nested_text(amount, "amount", "value")
                row["supply_usd"] = _nested_text(amount, "exchange", "value")
                row["supply_apy_percent"] = _nested_text(
                    reserve, "summary", "supplyApy", "normalized"
                )
                row["supply_principal_amount"] = _nested_text(
                    item, "principal", "amount", "value"
                )
                row["supply_interest_amount"] = _nested_text(
                    item, "interest", "amount", "value"
                )
                row["is_collateral"] = str(bool(item.get("isCollateral"))).lower()
                row["can_be_collateral"] = str(
                    bool(reserve.get("canUseAsCollateral"))
                ).lower()
            else:
                row["borrow_amount"] = _nested_text(amount, "amount", "value")
                row["borrow_usd"] = _nested_text(amount, "exchange", "value")
                row["borrow_apy_percent"] = _nested_text(
                    reserve, "summary", "borrowApy", "normalized"
                )
                row["borrow_principal_amount"] = _nested_text(
                    item, "principal", "amount", "value"
                )
                row["borrow_interest_amount"] = _nested_text(
                    item, "interest", "amount", "value"
                )

    for row in rows_by_position.values():
        row["net_usd"] = _format_decimal(
            _decimal_or_zero(row.get("supply_usd"))
            - _decimal_or_zero(row.get("borrow_usd"))
        )

    return sorted(rows_by_position.values(), key=lambda row: row["position_id"])


async def _fetch_aave_v4_rows(
    client: httpx.AsyncClient,
    wallet: str,
    chain_id: int,
) -> list[dict[str, str]]:
    user_chains = {"user": wallet, "chainIds": [chain_id]}
    try:
        data = await _fetch_graphql(
            client,
            AAVE_V4_POSITIONS_QUERY,
            {
                "positionsRequest": {
                    "user": wallet,
                    "filter": {"chainIds": [chain_id]},
                },
                "suppliesRequest": {"query": {"userChains": user_chains}},
                "borrowsRequest": {"query": {"userChains": user_chains}},
            },
            AAVE_V4_API_URL,
        )
        return _parse_v4_positions(wallet, chain_id, data)
    except HTTPException:
        rows = await _fetch_aave_v4_onchain_rows(client, wallet, chain_id)
        if rows:
            return rows
        raise


async def _fetch_aave_rows(
    client: httpx.AsyncClient,
    wallet: str,
    chain_id: int,
) -> list[dict[str, str]]:
    fetchers = [_fetch_aave_v3_rows(client, wallet, chain_id)]
    if chain_id in AAVE_V4_CHAIN_IDS:
        fetchers.append(_fetch_aave_v4_rows(client, wallet, chain_id))

    results = await asyncio.gather(*fetchers, return_exceptions=True)
    rows: list[dict[str, str]] = []
    errors: list[BaseException] = []
    for result in results:
        if isinstance(result, BaseException):
            errors.append(result)
        else:
            rows.extend(result)

    if rows:
        return sorted(rows, key=lambda row: row["position_id"])
    if errors:
        error = errors[-1]
        if isinstance(error, HTTPException):
            raise error
        raise HTTPException(status_code=502, detail="Aave API request failed") from error
    return []


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(AAVE_CSV_HEADER)
    for row in rows:
        writer.writerow([row.get(column, "") for column in AAVE_CSV_HEADER])
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export Aave V3 and V4 positions for Google Sheets",
    description=(
        "Returns normalized Aave V3 and V4 supply and borrow positions for an EVM "
        "wallet. Suitable for Google Sheets IMPORTDATA."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_aave_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(1, gt=0, description="Aave network chain ID."),
):
    wallet = _normalize_wallet(address)
    async with queued_async_client(timeout=20.0) as client:
        rows = await _fetch_aave_rows(client, wallet, chain_id)

    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={AAVE_CACHE_TTL_SECONDS}"},
    )
