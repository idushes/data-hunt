from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
import httpx
import logging
from database import get_db
from models import Account, AddressHistory, ProjectDict, TokenDict, CEXDict
from config import DEBANK_ACCESS_KEY
from dependencies import get_current_account

# Configure logging
logger = logging.getLogger(__name__)

from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel

router = APIRouter(
    prefix="/debank",
    tags=["debank"]
)

class TokenAmount(BaseModel):
    token_id: str
    symbol: str
    name: str # Token name
    logo_url: Optional[str] = None
    amount: float
    amount_raw: float # The raw amount (unsigned)
    value_usd: float
    price: float

class ReadableHistoryItem(BaseModel):
    tx_hash: str
    chain: str
    timestamp: float
    date_time: str
    
    cate_id: Optional[str] = None # e.g. 'send', 'receive'
    tx_name: Optional[str] = None # e.g. 'settle', 'mint'
    
    # Context / Counterparty
    project_id: Optional[str] = None
    project_name: Optional[str] = None
    project_logo_url: Optional[str] = None
    
    cex_id: Optional[str] = None
    cex_name: Optional[str] = None
    cex_logo_url: Optional[str] = None
    
    other_addr: Optional[str] = None
    wallet_addr: str 
    
    # Values
    usd_gas_fee: Optional[float] = None
    eth_gas_fee: Optional[float] = None # Native gas amount
    
    # Asset Changes (Negative = Sent, Positive = Received)
    token_changes: List[TokenAmount] = []
    
    description: str # Keep for fallback/simple usage
    is_scam: bool

@router.post("/all_history", summary="Update All Transaction History for User Addresses")
async def update_all_history(
    initial_sync_resume: bool = False,
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    """
    Updates the transaction history for all addresses linked to the authenticated user.
    Fetches data from Debank /v1/user/all_history_list using cursor-based pagination (start_time).
    Syncs AddressHistory, and updates ProjectDict, TokenDict, CEXDict.

    - **initial_sync_resume**: If True, continues fetching older history even if existing transactions are found (fills gaps).
    """
    if not account.addresses:
        raise HTTPException(status_code=400, detail="No addresses linked to this account")
    
    if account.balance <= 0:
        raise HTTPException(status_code=403, detail="Insufficient access balance")

    addresses = [addr.address for addr in account.addresses]
    results = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        headers = {"AccessKey": DEBANK_ACCESS_KEY}
        base_url = "https://pro-openapi.debank.com/v1/user/all_history_list"

        for address in addresses:
            # Sync history for this address
            synced_count = 0
            start_time = None
            has_error = False
            last_error = None
            
            # Keep fetching pages until we catch up or run out
            while True:
                params = {"id": address}
                if start_time is not None:
                    # Debank uses start_time as cursor (seconds). 
                    # If passed, returns history BEFORE this time.
                    params["start_time"] = int(start_time)

                try:
                    response = await client.get(base_url, params=params, headers=headers)
                    if response.status_code != 200:
                        has_error = True
                        last_error = f"API Error {response.status_code}: {response.text}"
                        break
                    
                    data = response.json()
                    history_list = data.get("history_list", [])
                    
                    # 1. Update Dictionaries (Upsert)
                    # We do this for every page to ensure we have latest metadata
                    
                    # ProjectDict
                    project_dict = data.get("project_dict", {})
                    for pid, pdata in project_dict.items():
                        # pdata has chain, id, logo_url, name, site_url
                        stmt = insert(ProjectDict).values(
                            id=pdata.get("id"),
                            chain=pdata.get("chain"),
                            logo_url=pdata.get("logo_url"),
                            name=pdata.get("name"),
                            site_url=pdata.get("site_url")
                        ).on_conflict_do_update(
                            index_elements=['id'],
                            set_={
                                "chain": pdata.get("chain"),
                                "logo_url": pdata.get("logo_url"),
                                "name": pdata.get("name"),
                                "site_url": pdata.get("site_url")
                            }
                        )
                        db.execute(stmt)

                    # TokenDict
                    token_dict = data.get("token_dict", {})
                    for tid, tdata in token_dict.items():
                         stmt = insert(TokenDict).values(
                            id=tdata.get("id"),
                            chain=tdata.get("chain"),
                            name=tdata.get("name"),
                            symbol=tdata.get("symbol"),
                            display_symbol=tdata.get("display_symbol"),
                            optimized_symbol=tdata.get("optimized_symbol"),
                            decimals=tdata.get("decimals"),
                            logo_url=tdata.get("logo_url"),
                            protocol_id=tdata.get("protocol_id"),
                            price=tdata.get("price"),
                            price_24h_change=tdata.get("price_24h_change"),
                            is_verified=tdata.get("is_verified"),
                            is_core=tdata.get("is_core"),
                            is_wallet=tdata.get("is_wallet"),
                            is_scam=tdata.get("is_scam"),
                            is_suspicious=tdata.get("is_suspicious"),
                            credit_score=tdata.get("credit_score"),
                            total_supply=tdata.get("total_supply"),
                            time_at=tdata.get("time_at")
                        ).on_conflict_do_update(
                            index_elements=['id'],
                            set_={k: tdata.get(k) for k in [
                                "chain", "name", "symbol", "display_symbol", "optimized_symbol",
                                "decimals", "logo_url", "protocol_id", "price", "price_24h_change",
                                "is_verified", "is_core", "is_wallet", "is_scam", "is_suspicious",
                                "credit_score", "total_supply", "time_at"
                            ] if k in tdata}
                        )
                         db.execute(stmt)

                    # CEXDict
                    cex_dict = data.get("cex_dict", {})
                    for cid, cdata in cex_dict.items():
                        # cid is the address (0x...), cdata['id'] is cex_id (e.g. coinbase)
                        stmt = insert(CEXDict).values(
                            id=cid,
                            cex_id=cdata.get("id"),
                            name=cdata.get("name"),
                            logo_url=cdata.get("logo_url"),
                            is_deposit=cdata.get("is_deposit"),
                            is_collect=cdata.get("is_collect"),
                            is_gastopup=cdata.get("is_gastopup"),
                            is_vault=cdata.get("is_vault"),
                            is_withdraw=cdata.get("is_withdraw")
                        ).on_conflict_do_update(
                            index_elements=['id'],
                            set_={
                                "cex_id": cdata.get("id"),
                                "name": cdata.get("name"),
                                "logo_url": cdata.get("logo_url"),
                                "is_deposit": cdata.get("is_deposit"),
                                "is_collect": cdata.get("is_collect"),
                                "is_gastopup": cdata.get("is_gastopup"),
                                "is_vault": cdata.get("is_vault"),
                                "is_withdraw": cdata.get("is_withdraw")
                            }
                        )
                        db.execute(stmt)
                    
                    db.commit() # Commit dict updates

                    if not history_list:
                        break # No more history
                    
                    # 2. Process History Items
                    # history_list is ordered most recent first
                    
                    first_exists = False
                    
                    for item in history_list:
                        tx_id = item.get("id")
                        chain_id = item.get("chain")
                        if not tx_id or not chain_id:
                            continue
                            
                        # Check if exists
                        exists = db.query(AddressHistory).filter(
                            AddressHistory.id == tx_id,
                            AddressHistory.chain == chain_id,
                            AddressHistory.address == address.lower()
                        ).first()
                        
                        if exists:
                            if initial_sync_resume:
                                # Continue processing list even if exists, to fill gaps
                                continue
                            else:
                                first_exists = True
                                # We found a transaction already in DB.
                                # Assume all subsequent (older) transactions in this list and future pages are also in DB.
                                # Stop processing this address.
                                break
                        
                        # Insert new
                        new_hist = AddressHistory(
                            id=tx_id,
                            chain=chain_id,
                            address=address.lower(),
                            cate_id=item.get("cate_id"),
                            time_at=int(item.get("time_at")) if item.get("time_at") else None,
                            is_scam=item.get("is_scam", False),
                            json=item # Store full row data
                        )
                        db.add(new_hist)
                        synced_count += 1
                    
                    db.commit() # Commit history updates
                    
                    if first_exists:
                        # Caught up with DB
                        break
                    
                    # Prepare for next page
                    # start_time for next request should be the time_at of the LAST item in current list
                    last_item_time = history_list[-1].get("time_at")
                    if last_item_time:
                        start_time = last_item_time
                    else:
                        break

                except Exception as e:
                    has_error = True
                    last_error = str(e)
                    logger.error(f"Error syncing history for {address}: {e}")
                    break
            
            results.append({
                "address": address,
                "status": "success" if not has_error else "partial_error",
                "synced_count": synced_count,
                "error": last_error
            })
    
    return {"results": results}


@router.get("/history/readable", response_model=List[ReadableHistoryItem], summary="Get Readable Transaction History")
async def get_readable_history(
    skip: int = 0,
    limit: int = 50,
    chain: Optional[str] = None,
    include_scam: bool = False,
    min_value_usd: float = 0.01,
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    """
    Get a human-readable list of transactions.
    Filters out 'token_approve' transactions by default.
    Filters out transactions with value < min_value_usd (default $0.01).
    Resolves token symbols using local TokenDict.
    """
    if not account.addresses:
        return []

    my_addresses = [addr.address.lower() for addr in account.addresses]
    query = db.query(AddressHistory).filter(AddressHistory.address.in_(my_addresses))

    # Filter by scam
    if not include_scam:
        query = query.filter(AddressHistory.is_scam == False)

    # Filter by chain
    if chain:
        query = query.filter(AddressHistory.chain == chain)

    # Sort by time_at desc
    query = query.order_by(AddressHistory.time_at.desc())
    
    result_items = []
    current_skip = skip
    chunk_size = limit * 2 # Fetch extra to account for filtering
    
    while len(result_items) < limit:
        chunk = query.offset(current_skip).limit(chunk_size).all()
        if not chunk:
            break
            
        current_skip += len(chunk)
        
        # Collect all referenced IDs for batch fetching
        token_ids = set()
        project_ids = set()
        cex_check_addrs = set()
        
        for hist in chunk:
            data = hist.json or {}
            
            if data.get("token_approve"): continue
                
            for s in data.get("sends", []):
                if s.get("token_id"): token_ids.add(s["token_id"])
            for r in data.get("receives", []):
                if r.get("token_id"): token_ids.add(r["token_id"])
            
            if data.get("project_id"):
                project_ids.add(data["project_id"])
                
            oth = data.get("other_addr")
            if oth: cex_check_addrs.add(oth)
            tx_data = data.get("tx", {})
            if tx_data.get("from_addr"): cex_check_addrs.add(tx_data["from_addr"])
            if tx_data.get("to_addr"): cex_check_addrs.add(tx_data["to_addr"])

        # Batch Fetch Dictionaries
        tokens = {}
        if token_ids:
            token_objs = db.query(TokenDict).filter(TokenDict.id.in_(token_ids)).all()
            tokens = {t.id: t for t in token_objs}
            
        projects = {}
        if project_ids:
            proj_objs = db.query(ProjectDict).filter(ProjectDict.id.in_(project_ids)).all()
            projects = {p.id: p for p in proj_objs}
            
        cex_map = {}
        if cex_check_addrs:
             cex_objs = db.query(CEXDict).filter(CEXDict.id.in_(cex_check_addrs)).all()
             cex_map = {c.id.lower(): c for c in cex_objs}

        # Helper to hydrate token
        def get_token_info(tid, amount):
            t = tokens.get(tid)
            symbol = "???"
            name = "Unknown"
            logo = None
            price = 0.0
            
            if t:
                symbol = t.optimized_symbol or t.display_symbol or t.symbol or "???"
                name = t.name or symbol
                logo = t.logo_url
                price = float(t.price) if t.price else 0.0
            
            val = amount * price
            return TokenAmount(
                token_id=tid,
                symbol=symbol,
                name=name,
                logo_url=logo,
                amount=amount, # Signed already passed in? No, we pass logic amount
                amount_raw=abs(amount),
                value_usd=val,
                price=price
            )

        processed_hashes = set()
        grouped_results = []
        
        # Group items by hash from the current chunk
        # Note: This simple approach processes the chunk. If a pair is split across chunks check (unlikely with time sort), 
        # it might show up twice in total list but okay for pagination.
        
        # We perform the hydration for ALL items first, then deduplicate
        
        temp_items = []

        for hist in chunk:
            if len(result_items) >= limit: # This check needs to be conceptually applied to *groups*
                # But we can't easily stop early if we need to group.
                # Let's just process the chunk and append to result_items until full
                pass

            data = hist.json or {}
            if data.get("token_approve"): continue

            sends = data.get("sends", [])
            receives = data.get("receives", [])
            
            # Prepare Token Changes
            token_changes = []
            
            # Sends (Negative amount)
            sent_value = 0.0
            for s in sends:
                t_obj = get_token_info(s.get("token_id"), -s.get("amount", 0))
                token_changes.append(t_obj)
                sent_value += abs(t_obj.value_usd)
                
            # Receives (Positive amount)
            recv_value = 0.0
            for r in receives:
                t_obj = get_token_info(r.get("token_id"), r.get("amount", 0))
                token_changes.append(t_obj)
                recv_value += abs(t_obj.value_usd)
            
            # Value Filter
            if (sent_value + recv_value) < min_value_usd:
                continue

            # Identify Wallet Perspective
            my_address_set = set(my_addresses)
            wallet_addr = hist.address # Guaranteed to be the owner for this row
            
            tx_data = data.get("tx", {})
            from_addr = tx_data.get("from_addr", "").lower()
            to_addr = tx_data.get("to_addr", "").lower()
            
            is_sender = False
            if from_addr == wallet_addr:
                is_sender = True
            elif any(s.get("to_addr", "").lower() == wallet_addr for s in sends):
                 is_sender = False
            elif receives:
                 is_sender = False
            elif sends:
                 is_sender = True

            # Resolve Project
            pid = data.get("project_id")
            p_obj = projects.get(pid) if pid else None
            
            # Resolve CEX / Counterparty
            other_addr = data.get("other_addr")
            if not other_addr and is_sender: other_addr = to_addr
            if not other_addr and not is_sender: other_addr = from_addr
            
            cex_obj = None
            if other_addr:
                 cex_obj = cex_map.get(other_addr.lower())
            
            description = ""
            if sends and receives:
                description = "Swap" 
            elif sends:
                description = "Send"
            elif receives:
                description = "Receive"
            else:
                description = tx_data.get("name", "Interaction")

            dt_str = ""
            if hist.time_at:
                dt_str = datetime.fromtimestamp(hist.time_at).strftime("%Y-%m-%d %H:%M:%S")

            item = ReadableHistoryItem(
                tx_hash=hist.id,
                chain=hist.chain,
                timestamp=hist.time_at or 0.0,
                date_time=dt_str,
                cate_id=hist.cate_id,
                tx_name=tx_data.get("name"),
                project_id=pid,
                project_name=p_obj.name if p_obj else None,
                project_logo_url=p_obj.logo_url if p_obj else None,
                cex_id=cex_obj.cex_id if cex_obj else None,
                cex_name=cex_obj.name if cex_obj else None,
                cex_logo_url=cex_obj.logo_url if cex_obj else None,
                other_addr=other_addr,
                wallet_addr=wallet_addr,
                usd_gas_fee=tx_data.get("usd_gas_fee"),
                eth_gas_fee=tx_data.get("eth_gas_fee"),
                token_changes=token_changes,
                description=description, 
                is_scam=hist.is_scam or False
            )
            temp_items.append(item)

        # Deduplicate Logic
        # We group items by (tx_hash, chain).
        # Should we assume time-sort keeps them close? Yes.
        
        grouped = {}
        for item in temp_items:
            key = (item.tx_hash, item.chain)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(item)
            
        # Process groups
        for key, group in grouped.items():
            if len(result_items) >= limit:
                break
                
            selected_item = group[0]
            
            if len(group) > 1:
                # Collision - likely self transfer or multi-party
                # Prefer the Sender side (has gas fees usually)
                sender_ver = next((i for i in group if i.description == "Send" or i.cate_id == "send"), None)
                if sender_ver:
                    selected_item = sender_ver
                    # Check if 'other_addr' matches one of our addresses
                    # If so, label as Self Transfer?
                    # But other_addr might be the Contract.
                    # Usually for simple transfer, other_addr is the recipient.
                    if selected_item.other_addr and selected_item.other_addr.lower() in my_address_set:
                        selected_item.description = "Self Transfer"
                else:
                    # Prefer the one with gas fee
                    with_gas = next((i for i in group if i.usd_gas_fee and i.usd_gas_fee > 0), None)
                    if with_gas: selected_item = with_gas

            result_items.append(selected_item)
            
    return result_items
