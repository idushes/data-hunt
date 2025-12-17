import logging
from fastapi import APIRouter, HTTPException
from utils import load_chains

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/chains", tags=["Chains"], summary="Get Available Chains")
async def get_chains():
    """
    Returns the list of available chains supported by the application.
    """
    chains = load_chains()
    if not chains:
        raise HTTPException(status_code=500, detail="Could not load chain list.")
    return chains
