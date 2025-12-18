import json
import logging
from typing import List, Dict, Set

logger = logging.getLogger(__name__)

_chains_cache = None
_valid_chain_ids_cache = None

def load_chains() -> List[Dict]:
    """
    Loads the list of available chains from docs/debank_chain_list.json.
    Returns a list of dictionaries.
    """
    global _chains_cache
    if _chains_cache is not None:
        return _chains_cache

    try:
        with open("docs/debank_chain_list.json", "r") as f:
            chains = json.load(f)
            _chains_cache = chains
            return chains
    except FileNotFoundError:
        logger.error("docs/debank_chain_list.json not found.")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding chain list JSON: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error loading chains: {e}")
        return []

def get_valid_chain_ids() -> Set[str]:
    """
    Returns a set of valid chain IDs.
    """
    global _valid_chain_ids_cache
    if _valid_chain_ids_cache is not None:
        return _valid_chain_ids_cache
    
    chains = load_chains()
    _valid_chain_ids_cache = {chain.get("id") for chain in chains if chain.get("id")}
    return _valid_chain_ids_cache

from sqlalchemy.orm import Session
from models import DebankRequest

import time

def get_latest_debank_data(db: Session, account_id: str, valid_addresses: List[str] = None, path: str = "/v1/user/all_complex_protocol_list") -> List[tuple]:
    """
    Fetches the latest successful Debank API response for each address linked to the account.
    Returns a list of tuples: (address, data_json).
    Only considers requests from the last 7 days.
    If valid_addresses is provided, filters results to only include these addresses.
    """
    # 7 days in seconds
    seven_days_ago = int(time.time() - (7 * 24 * 60 * 60))

    # Fetch all successful requests for this account within last 7 days
    # Ordered by Created At DESC (Newest First)
    # This ensures that when we iterate, we encounter the MOST RECENT data for an address first.
    requests = db.query(DebankRequest).filter(
        DebankRequest.account_id == account_id,
        DebankRequest.status == "success",
        DebankRequest.path == path,
        DebankRequest.created_at >= seven_days_ago
    ).order_by(DebankRequest.created_at.desc()).all()
    
    # Normalize valid_addresses for comparison
    valid_addresses_set = set(a.lower() for a in valid_addresses) if valid_addresses else None
    
    # Deduplicate by address found in params
    unique_data = {}
    
    for req in requests:
        try:
            if not req.params:
                continue
            params = json.loads(req.params)
            address = params.get("id")
            if not address:
                continue
            
            # Normalize address
            address = address.lower()
            
            # Filter by known active addresses
            if valid_addresses_set is not None and address not in valid_addresses_set:
                continue
            
            # If we haven't seen this address yet, it's the NEWEST one (due to DESC sorting).
            # Save it and ignore subsequent (older) entries for this address.
            if address not in unique_data:
                # Parse response
                if req.response_json:
                    data = json.loads(req.response_json)
                    unique_data[address] = data
        except Exception as e:
            logger.error(f"Error parsing request {req.id}: {e}")
            continue
            
    # Return list of (address, data)
    return list(unique_data.items())

import httpx
from typing import Optional
from config import DEBANK_ACCESS_KEY

async def fetch_debank_complex_protocols(db: Session, client: httpx.AsyncClient, account_id: str, address: str) -> dict:
    """
    Fetches complex protocol list from Debank for a given address.
    Handles DB logging (pending -> success/error).
    Returns a result dict with status and data/error.
    """
    # Create Pending Request
    db_request = DebankRequest(
        account_id=account_id,
        path="/v1/user/all_complex_protocol_list",
        params=json.dumps({"id": address}),
        status="pending",
        created_at=int(time.time()),
        response_json=None
    )
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    
    url = "https://pro-openapi.debank.com/v1/user/all_complex_protocol_list"
    params = {"id": address}
    headers = {"AccessKey": DEBANK_ACCESS_KEY}
    
    try:
        response = await client.get(url, params=params, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            db_request.status = "success"
            db_request.response_json = json.dumps(data)
            db.commit()
            return {
                "address": address,
                "status": "success",
                "data_count": len(data) if isinstance(data, list) else 0,
                "data": data
            }
        else:
            error_msg = response.text
            db_request.status = "error"
            db_request.response_json = error_msg
            db.commit()
            return {
                "address": address,
                "status": "error",
                "error": f"Debank API Error: {response.status_code}",
                "response": error_msg
            }
            
    except Exception as e:
        logger.error(f"Error fetching data for {address}: {e}")
        db_request.status = "error"
        db_request.response_json = str(e)
        db.commit()
        return {
            "address": address,
            "status": "error",
            "error": str(e)
        }

async def fetch_debank_token_list(db: Session, client: httpx.AsyncClient, account_id: str, address: str) -> dict:
    """
    Fetches token list from Debank for a given address.
    Handles DB logging (pending -> success/error).
    Returns a result dict with status and data/error.
    """
    # Create Pending Request
    db_request = DebankRequest(
        account_id=account_id,
        path="/v1/user/all_token_list",
        params=json.dumps({"id": address, "is_all": False}),
        status="pending",
        created_at=int(time.time()),
        response_json=None
    )
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    
    url = "https://pro-openapi.debank.com/v1/user/all_token_list"
    params = {"id": address, "is_all": "false"}
    headers = {"AccessKey": DEBANK_ACCESS_KEY}
    
    try:
        response = await client.get(url, params=params, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            db_request.status = "success"
            db_request.response_json = json.dumps(data)
            db.commit()
            return {
                "address": address,
                "status": "success",
                "data_count": len(data) if isinstance(data, list) else 0,
                "data": data
            }
        else:
            error_msg = response.text
            db_request.status = "error"
            db_request.response_json = error_msg
            db.commit()
            return {
                "address": address,
                "status": "error",
                "error": f"Debank API Error: {response.status_code}",
                "response": error_msg
            }
            
    except Exception as e:
        logger.error(f"Error fetching token list for {address}: {e}")
        db_request.status = "error"
        db_request.response_json = str(e)
        db.commit()
        return {
            "address": address,
            "status": "error",
            "error": str(e)
        }
