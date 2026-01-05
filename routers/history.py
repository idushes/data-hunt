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

router = APIRouter(
    prefix="/debank",
    tags=["debank"]
)

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
                            AddressHistory.chain == chain_id
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
