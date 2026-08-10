import json
import logging
from typing import Dict, List, Set

logger = logging.getLogger(__name__)

_chains_cache = None
_valid_chain_ids_cache = None


def load_chains() -> List[Dict]:
    """Load the chain metadata used by authentication and the public API."""
    global _chains_cache
    if _chains_cache is not None:
        return _chains_cache

    try:
        with open("docs/chain_list.json", "r") as file:
            chains = json.load(file)
            _chains_cache = chains
            return chains
    except FileNotFoundError:
        logger.error("docs/chain_list.json not found.")
        return []
    except json.JSONDecodeError as error:
        logger.error("Error decoding chain list JSON: %s", error)
        return []
    except Exception as error:
        logger.error("Unexpected error loading chains: %s", error)
        return []


def get_valid_chain_ids() -> Set[str]:
    """Return the chain IDs accepted when linking an account address."""
    global _valid_chain_ids_cache
    if _valid_chain_ids_cache is not None:
        return _valid_chain_ids_cache

    chains = load_chains()
    _valid_chain_ids_cache = {
        chain["id"] for chain in chains if isinstance(chain.get("id"), str)
    }
    return _valid_chain_ids_cache
