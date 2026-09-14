"""Read-only Curve crvUSD WBTC mint positions on Ethereum.

Contract addresses and units follow curvefi/curve-llamalend.js:
src/constants/llammas.ts and src/mintMarkets/MintMarketTemplate.ts.
"""
import csv
import io
import os
import re
from decimal import Decimal, localcontext

import httpx
from eth_abi import decode, encode
from eth_abi.exceptions import DecodingError
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from web3 import Web3

from outbound_queue import queued_async_client

CONTROLLER = "0x4e59541306910ad6dc1dac0ac9dfb29bd9f15c67"
AMM = "0xe0438eb3703bf871e31ce639bd351109c88666ea"
WBTC = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
CURVE_CSV_HEADER = [
    "wallet", "chain_id", "chain", "protocol", "market", "market_address",
    "position_id", "block_number", "collateral_symbol", "collateral_address",
    "collateral_amount", "stablecoin_symbol", "stablecoin_amount", "debt_amount",
    "oracle_price_crvusd", "collateral_value_crvusd", "position_value_crvusd",
    "net_value_crvusd", "health_percent", "borrow_apr_percent",
    "liquidation_range_lower_crvusd", "liquidation_range_upper_crvusd",
    "distance_to_liquidation_percent", "band_count", "is_soft_liquidation",
    "is_liquidatable",
]
router = APIRouter(prefix="/curve", tags=["curve"])


def _wallet(address: str) -> str:
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", address.strip()):
        raise HTTPException(400, "Address must be a 42-character EVM hex address")
    return address.strip().lower()


async def _rpc(client, url, payload):
    try:
        response = await client.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(502, "Curve RPC request failed") from exc


async def _read(client, url, block, calls):
    payload = []
    for index, (target, signature, inputs, args, outputs) in enumerate(calls):
        data = Web3.keccak(text=signature)[:4] + encode(inputs, args)
        payload.append({"jsonrpc": "2.0", "id": index, "method": "eth_call",
                        "params": [{"to": target, "data": "0x" + data.hex()}, block]})
    results = await _rpc(client, url, payload)
    try:
        if not isinstance(results, list) or len(results) != len(calls):
            raise ValueError("invalid batch")
        by_id = {item["id"]: item for item in results}
        if set(by_id) != set(range(len(calls))):
            raise ValueError("invalid batch IDs")
        values = []
        for index, call in enumerate(calls):
            item = by_id[index]
            result = item.get("result")
            if "error" in item or not isinstance(result, str) or not result.startswith("0x"):
                raise ValueError("invalid result")
            values.append(decode(call[4], bytes.fromhex(result[2:])))
        return values
    except (KeyError, TypeError, ValueError, DecodingError) as exc:
        raise HTTPException(502, "Curve RPC returned invalid contract data") from exc


def _format(value):
    return "0" if value == 0 else format(value.normalize(), "f")


def _position_row(wallet, block, values):
    state, health, prices, oracle, rate = values
    with localcontext() as ctx:
        ctx.prec = 78
        collateral = Decimal(state[0][0]) / 10**8
        stablecoin = Decimal(state[0][1]) / 10**18
        debt = Decimal(state[0][2]) / 10**18
        price = Decimal(oracle[0]) / 10**18
        lower, upper = sorted(Decimal(p) / 10**18 for p in prices[0])
        collateral_value = collateral * price
        total_value = collateral_value + stablecoin
        return {
            "wallet": wallet, "chain_id": "1", "chain": "Ethereum",
            "protocol": "Curve crvUSD", "market": "WBTC/crvUSD Mint",
            "market_address": CONTROLLER, "position_id": f"1:{CONTROLLER}:{wallet}",
            "block_number": str(int(block, 16)), "collateral_symbol": "WBTC",
            "collateral_address": WBTC, "collateral_amount": _format(collateral),
            "stablecoin_symbol": "crvUSD", "stablecoin_amount": _format(stablecoin),
            "debt_amount": _format(debt), "oracle_price_crvusd": _format(price),
            "collateral_value_crvusd": _format(collateral_value),
            "position_value_crvusd": _format(total_value),
            "net_value_crvusd": _format(total_value - debt),
            "health_percent": _format(Decimal(health[0]) / 10**16),
            "borrow_apr_percent": _format(Decimal(rate[0]) * 31_536_000 / 10**16),
            "liquidation_range_lower_crvusd": _format(lower),
            "liquidation_range_upper_crvusd": _format(upper),
            "distance_to_liquidation_percent": _format((price - upper) / price * 100) if price else "",
            "band_count": str(state[0][3]),
            "is_soft_liquidation": str(bool(debt and stablecoin)).lower(),
            "is_liquidatable": str(health[0] < 0).lower(),
        }


async def _fetch_curve_rows(client, wallet):
    url = os.getenv("CURVE_ETHEREUM_RPC_URL") or "https://ethereum-rpc.publicnode.com"
    response = await _rpc(client, url, {"jsonrpc": "2.0", "id": 0, "method": "eth_blockNumber", "params": []})
    block = response.get("result") if isinstance(response, dict) else None
    if not isinstance(block, str) or not re.fullmatch(r"0x[0-9a-fA-F]+", block) or "error" in response:
        raise HTTPException(502, "Curve RPC returned invalid block number")
    exists = await _read(client, url, block, [
        (CONTROLLER, "loan_exists(address)", ["address"], [wallet], ["bool"]),
    ])
    if not exists[0][0]:
        return []
    values = await _read(client, url, block, [
        (CONTROLLER, "user_state(address)", ["address"], [wallet], ["uint256[4]"]),
        (CONTROLLER, "health(address,bool)", ["address", "bool"], [wallet, True], ["int256"]),
        (CONTROLLER, "user_prices(address)", ["address"], [wallet], ["uint256[2]"]),
        (AMM, "price_oracle()", [], [], ["uint256"]),
        (AMM, "rate()", [], [], ["uint256"]),
    ])
    return [_position_row(wallet, block, values)]


@router.get("/positions.csv", summary="Export Curve WBTC/crvUSD mint position",
            description="Ethereum WBTC mint market only. Values are in crvUSD, not USD. Health is Curve's full health percentage; price bounds describe the soft-liquidation range.",
            responses={200: {"content": {"text/csv": {}}}})
async def get_curve_positions_csv(
    address: str = Query(..., description="EVM wallet address."),
    chain_id: int = Query(1, description="Ethereum (1) only."),
):
    wallet = _wallet(address)
    if chain_id != 1:
        raise HTTPException(400, "Curve WBTC mint positions support Ethereum (1) only")
    async with queued_async_client(timeout=45.0, trust_env=False) as client:
        rows = await _fetch_curve_rows(client, wallet)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CURVE_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return Response(output.getvalue(), media_type="text/csv", headers={"Cache-Control": "public, max-age=60"})
