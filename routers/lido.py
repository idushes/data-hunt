import asyncio
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


LIDO_CACHE_TTL_SECONDS = 60
LIDO_RPC_URL = "https://ethereum-rpc.publicnode.com"
LIDO_APR_URL = "https://eth-api.lido.fi/v1/protocol/steth/apr/sma"
STETH_ADDRESS = "0xae7ab96520de3a18e5e111b5eaab095312d7fe84"
WSTETH_ADDRESS = "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0"
WITHDRAWAL_QUEUE_ADDRESS = "0x889edc2edab5f40e902b864ad4d7ade8e412f9b1"
LIDO_CSV_HEADER = [
    "wallet",
    "chain_id",
    "chain",
    "protocol",
    "position_id",
    "position_type",
    "token_address",
    "token_symbol",
    "amount",
    "steth_equivalent",
    "staking_apr_percent",
    "withdrawal_request_id",
    "withdrawal_timestamp",
    "is_finalized",
    "is_claimed",
    "is_claimable",
]

router = APIRouter(prefix="/lido", tags=["lido"])


def _normalize_wallet(address: str) -> str:
    normalized = address.strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", normalized):
        raise HTTPException(
            status_code=400,
            detail="Address must be a 42-character EVM hex address",
        )
    return normalized.lower()


def _call_data(signature: str, types=None, values=None) -> str:
    payload = Web3.keccak(text=signature)[:4]
    if types:
        payload += encode(types, values or [])
    return "0x" + payload.hex()


async def _rpc_batch(
    client: httpx.AsyncClient,
    calls: list[tuple[str, str, list[str]]],
) -> list[tuple[Any, ...]]:
    rpc_url = os.getenv("LIDO_ETHEREUM_RPC_URL") or LIDO_RPC_URL
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
        raise HTTPException(status_code=502, detail="Lido RPC request failed") from exc
    if not isinstance(values, list):
        raise HTTPException(status_code=502, detail="Lido RPC returned invalid data")
    by_id = {
        item.get("id"): item
        for item in values
        if isinstance(item, dict) and isinstance(item.get("id"), int)
    }
    decoded = []
    for index, (_address, _data, types) in enumerate(calls):
        result = by_id.get(index, {}).get("result")
        try:
            decoded.append(decode(types, bytes.fromhex(result[2:])))
        except (AttributeError, TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=502, detail="Lido RPC returned undecodable data"
            ) from exc
    return decoded


async def _fetch_apr(client: httpx.AsyncClient) -> str:
    try:
        response = await client.get(LIDO_APR_URL)
        response.raise_for_status()
        payload = response.json()
        value = payload.get("data", {}).get("smaApr")
        return "" if value is None else str(value)
    except (AttributeError, httpx.HTTPError, ValueError):
        return ""


def _decimal(value: object | None) -> Decimal:
    try:
        return Decimal(str(value)) if value is not None else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _amount(raw: int) -> str:
    value = Decimal(raw) / Decimal(10**18)
    return "0" if value == 0 else format(value.normalize(), "f")


def _base_row(wallet: str, apr: str) -> dict[str, str]:
    row = {column: "" for column in LIDO_CSV_HEADER}
    row.update(
        {
            "wallet": wallet,
            "chain_id": "1",
            "chain": "Ethereum",
            "protocol": "Lido",
            "staking_apr_percent": apr,
        }
    )
    return row


async def _fetch_lido_rows(
    client: httpx.AsyncClient, wallet: str
) -> list[dict[str, str]]:
    wallet_arg = [Web3.to_checksum_address(wallet)]
    initial_task = _rpc_batch(
        client,
        [
            (
                STETH_ADDRESS,
                _call_data("balanceOf(address)", ["address"], wallet_arg),
                ["uint256"],
            ),
            (
                WSTETH_ADDRESS,
                _call_data("balanceOf(address)", ["address"], wallet_arg),
                ["uint256"],
            ),
            (
                WITHDRAWAL_QUEUE_ADDRESS,
                _call_data(
                    "getWithdrawalRequests(address)", ["address"], wallet_arg
                ),
                ["uint256[]"],
            ),
        ],
    )
    initial, apr = await asyncio.gather(initial_task, _fetch_apr(client))
    steth_raw = int(initial[0][0])
    wsteth_raw = int(initial[1][0])
    request_ids = list(initial[2][0])

    followup_calls = [
        (
            WSTETH_ADDRESS,
            _call_data(
                "getStETHByWstETH(uint256)", ["uint256"], [wsteth_raw]
            ),
            ["uint256"],
        )
    ]
    if request_ids:
        followup_calls.append(
            (
                WITHDRAWAL_QUEUE_ADDRESS,
                _call_data(
                    "getWithdrawalStatus(uint256[])",
                    ["uint256[]"],
                    [request_ids],
                ),
                ["(uint256,uint256,address,uint256,bool,bool)[]"],
            )
        )
    followup = await _rpc_batch(client, followup_calls)
    wsteth_as_steth_raw = int(followup[0][0])

    rows = []
    if steth_raw:
        row = _base_row(wallet, apr)
        row.update(
            {
                "position_id": "steth",
                "position_type": "token",
                "token_address": STETH_ADDRESS,
                "token_symbol": "stETH",
                "amount": _amount(steth_raw),
                "steth_equivalent": _amount(steth_raw),
            }
        )
        rows.append(row)
    if wsteth_raw:
        row = _base_row(wallet, apr)
        row.update(
            {
                "position_id": "wsteth",
                "position_type": "token",
                "token_address": WSTETH_ADDRESS,
                "token_symbol": "wstETH",
                "amount": _amount(wsteth_raw),
                "steth_equivalent": _amount(wsteth_as_steth_raw),
            }
        )
        rows.append(row)

    statuses = list(followup[1][0]) if request_ids else []
    for request_id, status in zip(request_ids, statuses, strict=False):
        amount_raw, _shares, _owner, timestamp, finalized, claimed = status
        row = _base_row(wallet, apr)
        row.update(
            {
                "position_id": f"withdrawal:{request_id}",
                "position_type": "withdrawal",
                "token_address": STETH_ADDRESS,
                "token_symbol": "stETH",
                "amount": _amount(int(amount_raw)),
                "steth_equivalent": _amount(int(amount_raw)),
                "withdrawal_request_id": str(request_id),
                "withdrawal_timestamp": str(timestamp),
                "is_finalized": str(bool(finalized)).lower(),
                "is_claimed": str(bool(claimed)).lower(),
                "is_claimable": str(bool(finalized) and not bool(claimed)).lower(),
            }
        )
        if not claimed:
            rows.append(row)
    return rows


def _render_csv(rows: list[dict[str, str]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=LIDO_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@router.get(
    "/positions.csv",
    summary="Export Lido positions",
    description=(
        "Returns stETH and wstETH balances, stETH equivalents, the official "
        "seven-day APR, and unclaimed withdrawal requests."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_lido_positions_csv(
    address: str = Query(..., description="Ethereum wallet address."),
):
    wallet = _normalize_wallet(address)
    async with queued_async_client(timeout=30.0, trust_env=False) as client:
        rows = await _fetch_lido_rows(client, wallet)
    return Response(
        content=_render_csv(rows),
        media_type="text/csv",
        headers={"Cache-Control": f"public, max-age={LIDO_CACHE_TTL_SECONDS}"},
    )
