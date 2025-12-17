from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
import httpx
import json
import time
import logging
from database import get_db
from models import DebankRequest, Account
from config import DEBANK_ACCESS_KEY
from dependencies import get_current_account

# Configure logging
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/debank",
    tags=["debank"]
)

@router.post("/all_complex_protocol_list", summary="Update Complex Protocol List for User Addresses")
async def update_all_complex_protocol_list(
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    """
    Updates the complex protocol list for all addresses linked to the authenticated user.
    Requires account balance > 0.
    """
    if not account.addresses:
        raise HTTPException(status_code=400, detail="No addresses linked to this account")
    
    if account.balance <= 0:
        raise HTTPException(status_code=403, detail="Insufficient access balance")

    # Extract addresses strictly from the account relationship
    addresses = [addr.address for addr in account.addresses]

    results = []
    
    async with httpx.AsyncClient() as client:
        # Import inside function to avoid circular imports if any, or just at top if clean
        from utils import fetch_debank_complex_protocols
        
        for address in addresses:
            result = await fetch_debank_complex_protocols(db, client, account.id, address)
            
            # Clean up result for response (remove 'data' to save bandwidth if not needed, or keep it?)
            # Router previously returned data_count but not full data in logic above? 
            # Looking at previous implementation... it appended {"address", "status", "data_count"}
            
            summary = {
                "address": result["address"],
                "status": result["status"]
            }
            if "error" in result:
                summary["error"] = result["error"]
            if "data_count" in result:
                summary["data_count"] = result["data_count"]
                
            results.append(summary)

    return {"results": results}

@router.post("/all_token_list", summary="Update All Token List for User Addresses")
async def update_all_token_list(
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    """
    Updates the token list for all addresses linked to the authenticated user.
    Requires account balance > 0.
    """
    if not account.addresses:
        raise HTTPException(status_code=400, detail="No addresses linked to this account")
    
    if account.balance <= 0:
        raise HTTPException(status_code=403, detail="Insufficient access balance")

    # Extract addresses strictly from the account relationship
    addresses = [addr.address for addr in account.addresses]

    results = []
    
    async with httpx.AsyncClient() as client:
        from utils import fetch_debank_token_list
        
        for address in addresses:
            result = await fetch_debank_token_list(db, client, account.id, address)
            
            summary = {
                "address": result["address"],
                "status": result["status"]
            }
            if "error" in result:
                summary["error"] = result["error"]
            if "data_count" in result:
                summary["data_count"] = result["data_count"]
                
            results.append(summary)

    return {"results": results}
