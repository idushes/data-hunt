import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import (
    CSV_CACHE_FLIGHT_TIMEOUT_SECONDS,
    CSV_CACHE_MAX_ENTRIES,
    CSV_CACHE_TTL_SECONDS,
    PORT,
    VALUE_RATE_LIMIT_ANONYMOUS,
    VALUE_RATE_LIMIT_AUTHENTICATED,
    VALUE_RATE_LIMIT_WINDOW_SECONDS,
)
from csv_cache import CSVCacheMiddleware
from redis_client import close_redis_client
from routers.aave import router as aave_router
from routers.auth import router as auth_router
from routers.chains import router as chains_router
from routers.cmc import router as cmc_router
from routers.coinbase import router as coinbase_router
from routers.compound import router as compound_router
from routers.euler import router as euler_router
from routers.feature_requests import router as feature_requests_router
from routers.fluid import router as fluid_router
from routers.gmx import router as gmx_router
from routers.health import router as health_router
from routers.hyperliquid import router as hyperliquid_router
from routers.jupiter import router as jupiter_router
from routers.lido import router as lido_router
from routers.lighter import router as lighter_router
from routers.morpho import router as morpho_router
from routers.paradex import router as paradex_router
from routers.solana import router as solana_router
from routers.stablecoins import router as stablecoins_router
from routers.stakedao import router as stakedao_router
from routers.uniswap import router as uniswap_router
from routers.value import router as value_router
from utils import load_chains
from value_rate_limit import ValueRateLimitMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logger = logging.getLogger(__name__)

    # Run database migrations
    logger.info("Running database migrations...")
    try:
        from alembic import command
        from alembic.config import Config

        alembic_cfg = Config("alembic.ini")
        command.upgrade(alembic_cfg, "head")
        logger.info("Database migrations applied successfully.")
    except Exception as e:
        logger.error(f"Error applying migrations: {e}")

    yield
    await close_redis_client()


def get_description_with_chains():
    base_desc = "API for Data Hunt project."
    try:
        chains = load_chains()

        chain_table = "\n\n## Available Chains\n\n| ID | Name | Community ID |\n|:---|:---|:---|\n"
        for chain in chains:
            chain_table += f"| `{chain.get('id')}` | {chain.get('name')} | {chain.get('community_id')} |\n"

        return base_desc + chain_table
    except Exception as e:
        logging.getLogger(__name__).warning(f"Failed to load chain list for docs: {e}")
        return base_desc


app = FastAPI(
    lifespan=lifespan, title="Data Hunt API", description=get_description_with_chains()
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    CSVCacheMiddleware,
    ttl_seconds=CSV_CACHE_TTL_SECONDS,
    max_entries=CSV_CACHE_MAX_ENTRIES,
    flight_timeout_seconds=CSV_CACHE_FLIGHT_TIMEOUT_SECONDS,
)
app.add_middleware(
    ValueRateLimitMiddleware,
    authenticated_limit=VALUE_RATE_LIMIT_AUTHENTICATED,
    anonymous_limit=VALUE_RATE_LIMIT_ANONYMOUS,
    window_seconds=VALUE_RATE_LIMIT_WINDOW_SECONDS,
)

app.include_router(cmc_router)
app.include_router(paradex_router)
app.include_router(lighter_router)
app.include_router(hyperliquid_router)
app.include_router(coinbase_router)
app.include_router(solana_router)
app.include_router(fluid_router)
app.include_router(aave_router)
app.include_router(uniswap_router)
app.include_router(stablecoins_router)
app.include_router(stakedao_router)
app.include_router(morpho_router)
app.include_router(compound_router)
app.include_router(euler_router)
app.include_router(lido_router)
app.include_router(jupiter_router)
app.include_router(gmx_router)
app.include_router(value_router)
app.include_router(feature_requests_router)

app.include_router(auth_router)
app.include_router(chains_router)
app.include_router(health_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
