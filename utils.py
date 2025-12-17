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
