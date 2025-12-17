import os
import json
import httpx
import time
from datetime import datetime
from sqlalchemy.orm import Session
from database import SessionLocal
from models import Account, DebankRequest
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
        

        async with httpx.AsyncClient() as client:
            from utils import fetch_debank_complex_protocols

            for account in accounts:
                if not account.addresses:
                    continue
                    
                for addr_obj in account.addresses:
                    address = addr_obj.address
                    
                    try:
                        logger.info(f"Fetching data for {address} (Account {account.id})...")
                        
                        result = await fetch_debank_complex_protocols(db, client, account.id, address)
                        
                        if result["status"] == "success":
                            logger.info(f"Success: {address}")
                        else:
                            logger.error(f"Error fetching {address}: {result.get('error')}")

                    except Exception as e:
                        logger.error(f"Exception fetching {address}: {str(e)}")
    
    except Exception as e:
        logger.error(f"Task Failed: {e}")
    finally:
        db.close()
    
    logger.info(f"[{datetime.now()}] Data fetch task completed.")
