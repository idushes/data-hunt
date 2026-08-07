import httpx
from datetime import datetime
from sqlalchemy.orm import Session
from database import SessionLocal
from models import Account
from config import DEBANK_ACCESS_KEY

import logging

logger = logging.getLogger(__name__)

async def fetch_and_save_data():
    logger.info(f"[{datetime.now()}] Starting data fetch task...")
    
    if not DEBANK_ACCESS_KEY:
        logger.error("Error: DEBANK_ACCESS_KEY is missing!")
        return

    db: Session = SessionLocal()
    try:
        # 1. Get solvent accounts
        accounts = db.query(Account).filter(Account.balance > 0).all()
        
        if not accounts:
            logger.info("No accounts with positive balance found. Skipping update.")
            return
            
        logger.info(f"Found {len(accounts)} solvent accounts.")
        
        # 2. Collect all unique addresses to update
        # We need to map address -> account_id to log correctly later?
        # Actually, an address might belong to multiple accounts (in theory, though usually 1-to-1 or 1-to-many unique)
        # But if we update per address, we can log it with the associated account(s).
        # Simplest approach: Iterate accounts -> addresses. Duplicate checks for efficiency?
        # If multiple accounts have same address, we update it once or twice? 
        # Debank limits are strict. Ideally we update each unique address once.
        # But we need to link the request to the account_id.
        # Let's iterate accounts and their addresses.
        

        async with httpx.AsyncClient(timeout=60.0) as client:
            from utils import fetch_debank_complex_protocols, fetch_debank_token_list
            from routers.history import (
                enrich_history_prices_for_account,
                sync_history_for_account,
            )

            for account in accounts:
                if not account.addresses:
                    continue
                    
                for addr_obj in account.addresses:
                    address = addr_obj.address
                    
                    try:
                        logger.info(f"Fetching data for {address} (Account {account.id})...")
                        
                        # 1. Fetch Complex Protocol List
                        result_complex = await fetch_debank_complex_protocols(db, client, account.id, address)
                        
                        if result_complex["status"] == "success":
                            logger.info(f"Success (Complex): {address}")
                        else:
                            logger.error(f"Error fetching complex protocols for {address}: {result_complex.get('error')}")

                        # 2. Fetch Token List
                        result_token = await fetch_debank_token_list(db, client, account.id, address)

                        if result_token["status"] == "success":
                            logger.info(f"Success (Token List): {address}")
                        else:
                            logger.error(f"Error fetching token list for {address}: {result_token.get('error')}")

                    except Exception as e:
                        logger.error(f"Exception fetching {address}: {str(e)}")

                try:
                    history_result = await sync_history_for_account(account, db)
                    history_errors = [
                        result
                        for result in history_result["results"]
                        if result["status"] != "success"
                    ]
                    synced_count = sum(
                        result["synced_count"] for result in history_result["results"]
                    )
                    if history_errors:
                        logger.error(
                            "History sync completed with %d address error(s) for account %s",
                            len(history_errors),
                            account.id,
                        )
                    else:
                        logger.info(
                            "Success (History): account %s, %d new transaction(s)",
                            account.id,
                            synced_count,
                        )

                    price_result = await enrich_history_prices_for_account(account, db)
                    if price_result["status"] == "partial_error":
                        logger.error(
                            "Price enrichment left %d transaction(s) pending for account %s",
                            price_result["transactions_pending"],
                            account.id,
                        )
                    else:
                        logger.info(
                            "Success (History Prices): account %s, %d transaction(s) updated",
                            account.id,
                            price_result.get("transactions_synced", 0),
                        )
                except Exception as e:
                    logger.exception(
                        "Exception syncing history for account %s: %s",
                        account.id,
                        e,
                    )
    
    except Exception as e:
        logger.error(f"Task Failed: {e}")
    finally:
        db.close()
    
    logger.info(f"[{datetime.now()}] Data fetch task completed.")
