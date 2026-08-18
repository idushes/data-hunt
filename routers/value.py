import base64
import csv
import hashlib
import io
import json
import time
from dataclasses import dataclass

import httpx
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from database import get_db
from dependencies import get_current_account
from models import Account, AccountValueResource, ValueResource
from value_rate_limit import (
    DATA_ACCESS_INTERNAL_HEADER,
    DATA_ACCESS_INTERNAL_TOKEN,
)


@dataclass(frozen=True)
class ValueSource:
    path: str
    key_column: str


VALUE_SOURCES = {
    "paradex": ValueSource("/paradex/balance", "account"),
    "lighter": ValueSource("/lighter/balance", "account_index"),
    "hyperliquid": ValueSource("/hyperliquid/balance", "account"),
    "coinbase": ValueSource("/coinbase/balance", "id"),
    "bybit": ValueSource("/bybit/account.csv", "id"),
    "binance": ValueSource("/binance/account.csv", "id"),
    "gmtrade-assets": ValueSource("/solana/gmtrade.csv", "mint"),
    "gmtrade-perps": ValueSource("/solana/gmtrade-perps.csv", "position_address"),
    "kamino-vaults": ValueSource("/solana/kamino.csv", "vault_address"),
    "kamino-positions": ValueSource("/solana/kamino-positions.csv", "vault_address"),
    "fluid": ValueSource("/fluid/positions.csv", "position_id"),
    "aave": ValueSource("/aave/positions.csv", "position_id"),
    "uniswap": ValueSource("/uniswap/positions.csv", "position_id"),
    "uniswap-v4": ValueSource("/uniswap/v4/positions.csv", "position_id"),
    "pancakeswap": ValueSource("/pancakeswap/positions.csv", "position_id"),
    "stablecoins": ValueSource("/stablecoins/balances.csv", "balance_id"),
    "stakedao": ValueSource("/stakedao/positions.csv", "position_id"),
    "morpho": ValueSource("/morpho/positions.csv", "position_id"),
    "compound": ValueSource("/compound/positions.csv", "position_id"),
    "euler": ValueSource("/euler/positions.csv", "position_id"),
    "lido": ValueSource("/lido/positions.csv", "position_id"),
    "jupiter-jlp": ValueSource("/jupiter/jlp.csv", "position_id"),
    "gmx": ValueSource("/gmx/positions.csv", "position_id"),
    "polymarket": ValueSource("/polymarket/positions.csv", "position_id"),
    "pendle": ValueSource("/pendle/positions.csv", "position_id"),
}
VALUE_CONTROL_PARAMS = {"source", "key", "column", "auth_token"}
DIRECT_VALUE_SOURCES = {"cmc-price": "/cmc/price.csv"}
RESOURCE_PARAMETER_NAMES = {
    "paradex": frozenset({"token", "account", "field"}),
    "lighter": frozenset({"token", "account", "accounts", "address", "field"}),
    "hyperliquid": frozenset({"address", "account", "field", "aggregate"}),
    "coinbase": frozenset(
        {"capsule", "intx_capsule", "include_zero", "include_portfolios"}
    ),
    "bybit": frozenset({"capsule", "region", "include_positions"}),
    "binance": frozenset({"capsule", "include_futures"}),
    "gmtrade-assets": frozenset({"wallet"}),
    "gmtrade-perps": frozenset({"wallet"}),
    "kamino-vaults": frozenset({"wallet"}),
    "kamino-positions": frozenset({"wallet", "vault", "name"}),
    "fluid": frozenset({"address", "chain_id"}),
    "aave": frozenset({"address", "chain_id"}),
    "uniswap": frozenset({"address", "chain_id", "include_closed"}),
    "uniswap-v4": frozenset({"address", "chain_id", "include_closed"}),
    "pancakeswap": frozenset({"address", "chain_id", "include_closed"}),
    "stablecoins": frozenset({"address", "chain_id", "wallet", "tron_address"}),
    "stakedao": frozenset({"address", "chain_id"}),
    "morpho": frozenset({"address", "chain_id"}),
    "compound": frozenset({"address", "chain_id"}),
    "euler": frozenset({"address", "chain_id"}),
    "lido": frozenset({"address"}),
    "jupiter-jlp": frozenset({"wallet"}),
    "gmx": frozenset({"address", "chain_id"}),
    "polymarket": frozenset({"address", "size_threshold"}),
    "pendle": frozenset({"address", "include_closed"}),
    "cmc-price": frozenset({"symbol", "id", "convert"}),
}
RESOURCE_CREDENTIAL_PARAMS = {
    "paradex": frozenset({"token"}),
    "lighter": frozenset({"token"}),
    "coinbase": frozenset({"capsule", "intx_capsule"}),
    "bybit": frozenset({"capsule"}),
    "binance": frozenset({"capsule"}),
}
RESOURCE_ID_MIN_BYTES = 9
RESOURCE_ID_MAX_BYTES = 16

router = APIRouter(tags=["values"])


class CreateValueResource(BaseModel):
    source: str = Field(min_length=1, max_length=64)
    key: str | None = Field(default=None, max_length=1024)
    column: str | None = Field(default=None, max_length=128)
    parameters: dict[str, str] = Field(default_factory=dict)


class CopiedValueResourceItem(BaseModel):
    id: str
    source: str
    key: str | None
    column: str | None
    parameters: dict[str, str]
    credential_parameters: list[str]
    first_copied_at: int
    last_copied_at: int
    copy_count: int


class CopiedValueResourcesPage(BaseModel):
    items: list[CopiedValueResourceItem]
    total: int
    limit: int
    offset: int


def _copied_resource_item(
    history: AccountValueResource,
    resource: ValueResource,
) -> CopiedValueResourceItem:
    parameters = resource.parameters if isinstance(resource.parameters, dict) else {}
    safe_parameters = {
        str(name): str(value)
        for name, value in parameters.items()
        if name not in RESOURCE_CREDENTIAL_PARAMS.get(resource.source, frozenset())
    }
    return CopiedValueResourceItem(
        id=resource.id,
        source=resource.source,
        key=resource.key,
        column=resource.column,
        parameters=safe_parameters,
        credential_parameters=sorted(
            RESOURCE_CREDENTIAL_PARAMS.get(resource.source, frozenset())
        ),
        first_copied_at=history.first_copied_at,
        last_copied_at=history.last_copied_at,
        copy_count=history.copy_count,
    )


def _resource_source_path(source: str) -> str | None:
    source_config = VALUE_SOURCES.get(source)
    if source_config is not None:
        return source_config.path
    return DIRECT_VALUE_SOURCES.get(source)


def _normalize_resource_request(
    request: CreateValueResource,
) -> tuple[str, str | None, str | None, dict[str, str]]:
    source = request.source.strip()
    if _resource_source_path(source) is None:
        supported = ", ".join(sorted(set(VALUE_SOURCES) | set(DIRECT_VALUE_SOURCES)))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported value source. Supported: {supported}",
        )

    key = request.key.strip() if request.key is not None else None
    column = request.column.strip() if request.column is not None else None
    key = key or None
    column = column or None
    if (key is None) != (column is None):
        raise HTTPException(
            status_code=400,
            detail="Stable value resources require both key and column",
        )
    if key is not None and source not in VALUE_SOURCES:
        raise HTTPException(
            status_code=400,
            detail="This source supports only a direct single-cell resource",
        )

    if len(request.parameters) > 20:
        raise HTTPException(status_code=400, detail="Too many source parameters")
    allowed = RESOURCE_PARAMETER_NAMES.get(source, frozenset())
    credentials = RESOURCE_CREDENTIAL_PARAMS.get(source, frozenset())
    parameters: dict[str, str] = {}
    for raw_name, raw_value in request.parameters.items():
        name = raw_name.strip()
        if name not in allowed:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported parameter '{name}' for source '{source}'",
            )
        if name in credentials:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Credential parameter '{name}' must not be stored. "
                    "Pass it only to the short value URL."
                ),
            )
        value = raw_value.strip()
        if len(value) > 2048:
            raise HTTPException(
                status_code=400,
                detail=f"Parameter '{name}' is too long",
            )
        if value:
            parameters[name] = value

    return source, key, column, dict(sorted(parameters.items()))


def _resource_fingerprint(
    source: str,
    key: str | None,
    column: str | None,
    parameters: dict[str, str],
) -> str:
    canonical = json.dumps(
        {
            "source": source,
            "key": key,
            "column": column,
            "parameters": parameters,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _resource_id_candidates(fingerprint: str):
    digest = bytes.fromhex(fingerprint)
    for size in range(RESOURCE_ID_MIN_BYTES, RESOURCE_ID_MAX_BYTES + 1):
        yield base64.urlsafe_b64encode(digest[:size]).decode().rstrip("=")


def _get_or_create_resource(
    db: Session,
    source: str,
    key: str | None,
    column: str | None,
    parameters: dict[str, str],
) -> ValueResource:
    fingerprint = _resource_fingerprint(source, key, column, parameters)
    existing = (
        db.query(ValueResource).filter(ValueResource.fingerprint == fingerprint).first()
    )
    if existing is not None:
        return existing

    for resource_id in _resource_id_candidates(fingerprint):
        collision = db.get(ValueResource, resource_id)
        if collision is not None:
            if collision.fingerprint == fingerprint:
                return collision
            continue

        resource = ValueResource(
            id=resource_id,
            fingerprint=fingerprint,
            source=source,
            key=key,
            column=column,
            parameters=parameters,
            created_at=int(time.time()),
        )
        db.add(resource)
        try:
            db.commit()
            db.refresh(resource)
            return resource
        except IntegrityError:
            db.rollback()
            existing = (
                db.query(ValueResource)
                .filter(ValueResource.fingerprint == fingerprint)
                .first()
            )
            if existing is not None:
                return existing

    raise HTTPException(
        status_code=409,
        detail="Could not allocate a unique short resource ID",
    )


def _source_error(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text or "Source request failed"

    if isinstance(payload, dict) and isinstance(payload.get("detail"), str):
        return payload["detail"]
    return response.text or "Source request failed"


def _render_single_cell(value: str) -> str:
    output = io.StringIO()
    csv.writer(output).writerow([value])
    return output.getvalue().rstrip("\r\n")


def _stable_key_candidates(source: str, key: str) -> tuple[str, ...]:
    if source != "stablecoins":
        return (key,)
    if key.startswith("ethereum:1:"):
        return (key, key.replace("ethereum:1:", "evm:1:", 1))
    if key.startswith("evm:1:"):
        return (key, key.replace("evm:1:", "ethereum:1:", 1))
    return (key,)


async def _request_source(
    request: Request,
    path: str,
    forwarded_query: list[tuple[str, str]],
) -> httpx.Response:
    forwarded_headers = {
        DATA_ACCESS_INTERNAL_HEADER: DATA_ACCESS_INTERNAL_TOKEN,
    }
    authorization = request.headers.get("authorization")
    if authorization:
        forwarded_headers["authorization"] = authorization

    transport = httpx.ASGITransport(app=request.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://data-hunt.internal",
        timeout=30.0,
    ) as client:
        return await client.get(
            path,
            params=forwarded_query,
            headers=forwarded_headers,
        )


def _raise_source_error(response: httpx.Response, source: str | None = None) -> None:
    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=_source_error(response),
            headers={"X-Value-Source": source} if source else None,
        )


async def _resolve_stable_value(
    request: Request,
    source: str,
    key: str,
    column: str,
    forwarded_query: list[tuple[str, str]],
) -> Response:
    source_config = VALUE_SOURCES.get(source)
    if source_config is None:
        supported = ", ".join(sorted(VALUE_SOURCES))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported value source. Supported: {supported}",
        )

    source_response = await _request_source(
        request,
        source_config.path,
        forwarded_query,
    )
    _raise_source_error(source_response, source)

    content_type = source_response.headers.get("content-type", "").lower()
    if not content_type.startswith("text/csv"):
        raise HTTPException(
            status_code=400,
            detail=(
                "The selected source returned a single value instead of a table. "
                "Use that source URL directly."
            ),
        )

    reader = csv.DictReader(io.StringIO(source_response.text))
    fieldnames = reader.fieldnames or []
    if source_config.key_column not in fieldnames:
        raise HTTPException(
            status_code=502,
            detail=f"Source CSV is missing key column '{source_config.key_column}'",
        )
    if column not in fieldnames:
        raise HTTPException(
            status_code=400,
            detail=f"Column '{column}' was not found in the source CSV",
        )

    key_candidates = _stable_key_candidates(source, key)
    matches = [
        row
        for row in reader
        if (row.get(source_config.key_column) or "") in key_candidates
    ]
    if not matches:
        raise HTTPException(
            status_code=404,
            detail=(f"Row with {source_config.key_column}='{key}' was not found"),
        )
    if len(matches) > 1:
        raise HTTPException(
            status_code=409,
            detail=(f"Stable key {source_config.key_column}='{key}' is not unique"),
        )

    return Response(
        content=_render_single_cell(matches[0].get(column) or ""),
        media_type="text/csv",
        headers={
            "X-Value-Source": source,
            "X-Value-Key-Column": source_config.key_column,
        },
    )


async def _resolve_direct_value(
    request: Request,
    source: str,
    forwarded_query: list[tuple[str, str]],
) -> Response:
    path = _resource_source_path(source)
    if path is None:
        raise HTTPException(status_code=400, detail="Unsupported value source")

    source_response = await _request_source(request, path, forwarded_query)
    _raise_source_error(source_response, source)
    content_type = source_response.headers.get("content-type", "").lower()
    if content_type.startswith("text/csv"):
        rows = [row for row in csv.reader(io.StringIO(source_response.text)) if row]
        if len(rows) != 1 or len(rows[0]) != 1:
            raise HTTPException(
                status_code=400,
                detail="The saved resource did not return exactly one cell",
            )
        value = rows[0][0]
    elif content_type.startswith("text/plain"):
        value = source_response.text.strip()
    else:
        raise HTTPException(
            status_code=400,
            detail="The saved resource did not return a CSV or plain-text value",
        )

    return Response(
        content=_render_single_cell(value),
        media_type="text/csv",
        headers={"X-Value-Source": source},
    )


@router.get(
    "/value",
    summary="Get one stable value for Google Sheets",
    description=(
        "Finds a row in a supported CSV source by its stable key and returns one "
        "selected column as a single-cell CSV. Row order and table size do not "
        "affect the result. All additional query parameters are forwarded to the "
        "selected source."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_stable_value(
    request: Request,
    source: str = Query(..., description="Registered CSV source alias."),
    key: str = Query(..., description="Exact stable row identifier."),
    column: str = Query(..., description="CSV column whose value should be returned."),
):
    forwarded_query = [
        (name, value)
        for name, value in request.query_params.multi_items()
        if name not in VALUE_CONTROL_PARAMS
    ]
    return await _resolve_stable_value(
        request,
        source,
        key,
        column,
        forwarded_query,
    )


@router.post(
    "/value-resources",
    summary="Create or reuse a short value resource",
    description=(
        "Stores a credential-free description of one requested value and returns "
        "a stable short ID. Identical requests reuse the same ID."
    ),
)
async def create_value_resource(
    payload: CreateValueResource,
    db: Session = Depends(get_db),
):
    source, key, column, parameters = _normalize_resource_request(payload)
    resource = _get_or_create_resource(
        db,
        source,
        key,
        column,
        parameters,
    )
    return {
        "id": resource.id,
        "credential_parameters": sorted(
            RESOURCE_CREDENTIAL_PARAMS.get(source, frozenset())
        ),
    }


@router.post(
    "/value-resources/{resource_id}/copies",
    response_model=CopiedValueResourceItem,
    summary="Record a copied value resource",
    description=(
        "Records a successful client-side copy for the current account without "
        "resolving the value or requesting its external source."
    ),
)
async def record_value_resource_copy(
    resource_id: str = Path(
        ...,
        min_length=12,
        max_length=22,
        pattern=r"^[A-Za-z0-9_-]+$",
    ),
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db),
):
    resource = db.get(ValueResource, resource_id)
    if resource is None:
        raise HTTPException(status_code=404, detail="Value resource not found")

    now = int(time.time())
    filters = (
        AccountValueResource.account_id == account.id,
        AccountValueResource.resource_id == resource.id,
    )
    updated = (
        db.query(AccountValueResource)
        .filter(*filters)
        .update(
            {
                AccountValueResource.last_copied_at: now,
                AccountValueResource.copy_count: AccountValueResource.copy_count + 1,
            },
            synchronize_session=False,
        )
    )
    if updated == 0:
        db.add(
            AccountValueResource(
                account_id=account.id,
                resource_id=resource.id,
                first_copied_at=now,
                last_copied_at=now,
                copy_count=1,
            )
        )
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            (
                db.query(AccountValueResource)
                .filter(*filters)
                .update(
                    {
                        AccountValueResource.last_copied_at: now,
                        AccountValueResource.copy_count: (
                            AccountValueResource.copy_count + 1
                        ),
                    },
                    synchronize_session=False,
                )
            )
            db.commit()
    else:
        db.commit()

    history = db.query(AccountValueResource).filter(*filters).one()
    return _copied_resource_item(history, resource)


@router.get(
    "/value-resources/mine",
    response_model=CopiedValueResourcesPage,
    summary="List copied value resources",
    description=(
        "Returns the current account's unique copied resources without credentials "
        "or authorization tokens, newest first."
    ),
)
async def list_copied_value_resources(
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db),
):
    history_query = db.query(AccountValueResource).filter(
        AccountValueResource.account_id == account.id
    )
    total = history_query.count()
    rows = (
        history_query.join(
            ValueResource,
            ValueResource.id == AccountValueResource.resource_id,
        )
        .with_entities(AccountValueResource, ValueResource)
        .order_by(
            AccountValueResource.last_copied_at.desc(),
            AccountValueResource.id.desc(),
        )
        .offset(offset)
        .limit(limit)
        .all()
    )
    return CopiedValueResourcesPage(
        items=[_copied_resource_item(history, resource) for history, resource in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/v/{resource_id}",
    summary="Get a saved value by short resource ID",
    description=(
        "Resolves a short resource ID and returns exactly one CSV cell. "
        "Credentials, when required, are accepted only as extra query parameters."
    ),
    responses={200: {"content": {"text/csv": {}}}},
)
async def get_value_resource(
    request: Request,
    resource_id: str = Path(
        ...,
        min_length=12,
        max_length=22,
        pattern=r"^[A-Za-z0-9_-]+$",
    ),
    db: Session = Depends(get_db),
):
    resource = db.get(ValueResource, resource_id)
    if resource is None:
        raise HTTPException(status_code=404, detail="Value resource not found")

    credentials = RESOURCE_CREDENTIAL_PARAMS.get(
        resource.source,
        frozenset(),
    )
    supplied_credentials: list[tuple[str, str]] = []
    for name, value in request.query_params.multi_items():
        if name == "auth_token":
            continue
        if name not in credentials:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported credential parameter '{name}'",
            )
        if len(value) > 65536:
            raise HTTPException(
                status_code=400,
                detail=f"Credential parameter '{name}' is too long",
            )
        supplied_credentials.append((name, value))

    stored_parameters = resource.parameters or {}
    forwarded_query = list(sorted(stored_parameters.items()))
    forwarded_query.extend(supplied_credentials)
    if resource.key is not None and resource.column is not None:
        return await _resolve_stable_value(
            request,
            resource.source,
            resource.key,
            resource.column,
            forwarded_query,
        )
    return await _resolve_direct_value(
        request,
        resource.source,
        forwarded_query,
    )
