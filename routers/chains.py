import json
import logging
from fastapi import APIRouter, HTTPException

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/chains", tags=["Chains"], summary="Get Available Chains")
async def get_chains():
    """
    Returns the list of available chains supported by the application.
    """
    try:
        with open("docs/debank_chain_list.json", "r") as f:
            chains = json.load(f)
        return chains
    except FileNotFoundError:
        logger.error("docs/debank_chain_list.json not found.")
        raise HTTPException(status_code=500, detail="Chain list file not found.")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding chain list JSON: {e}")
        raise HTTPException(status_code=500, detail="Error decoding chain list.")
    except Exception as e:
        logger.error(f"Unexpected error loading chains: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")
