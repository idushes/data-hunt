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
        headers = {"AccessKey": DEBANK_ACCESS_KEY}
        
        for address in addresses:
            # Create pending request record
            db_request = DebankRequest(
                account_id=account.id, # Link to user
                path="/v1/user/all_complex_protocol_list",
                params=json.dumps({"id": address}),
                status="pending",
                created_at=int(time.time()),
                response_json=None
            )
            db.add(db_request)
            db.commit()
            db.refresh(db_request)
            
            try:
                # Call Debank API
                url = "https://pro-openapi.debank.com/v1/user/all_complex_protocol_list"
                params = {"id": address}
                
                start_time = time.time()
                response = await client.get(url, params=params, headers=headers)
                end_time = time.time()
                
                # Check if successful
                if response.status_code == 200:
                    data = response.json()
                    
                    # Update DB record
                    db_request.status = "success"
                    db_request.response_json = json.dumps(data)
                    # Heuristic cost: Debank calls usually have a cost, but we might not know it from headers effortlessly unless we parse them.
                    # For now keep cost null or 0.
                    
                    db.commit()
                    
                    results.append({
                        "address": address,
                        "status": "success",
                        "data_count": len(data) if isinstance(data, list) else 0
                    })
                else:
                    error_msg = response.text
                    db_request.status = "error"
                    db_request.response_json = error_msg
                    db.commit()
                    
                    results.append({
                        "address": address,
                        "status": "error",
                        "error": f"Debank API Error: {response.status_code}"
                    })

            except Exception as e:
                logger.error(f"Error fetching data for {address}: {e}")
                db_request.status = "error"
                db_request.response_json = str(e)
                db.commit()
                
                results.append({
                    "address": address,
                    "status": "error",
                    "error": str(e)
                })

    return {"results": results}
