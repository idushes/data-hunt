import os
import sys
import logging
import uvicorn
from fastapi import FastAPI

from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import DEBANK_ACCESS_KEY, PORT, get_target_ids, get_scheduler_trigger_args, RUN_ON_STARTUP
from tasks import fetch_and_save_data

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
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

    # Startup logic
    if not DEBANK_ACCESS_KEY:
        logger.warning("DEBANK_ACCESS_KEY is not set in environment variables!")
    
    ids = get_target_ids()
    logger.info(f"Scheduler configured. Target IDs: {ids}")

    if RUN_ON_STARTUP:
        logger.info("RUN_ON_STARTUP is True. Executing fetch task now...")
        await fetch_and_save_data()

    trigger_args = get_scheduler_trigger_args()
    
    scheduler = AsyncIOScheduler()
    # Support both CronTrigger and IntervalTrigger based on config
    if trigger_args["trigger"] == "cron":
        trigger = CronTrigger(hour=trigger_args["hour"], minute=trigger_args["minute"])
        logger.info(f"Scheduler started. Task will run daily at {trigger_args['hour']:02d}:{trigger_args['minute']:02d}")
    else:
        # Remove 'trigger' key safely
        kwargs = trigger_args.copy()
        kwargs.pop("trigger")
        scheduler.add_job(fetch_and_save_data, "interval", **kwargs)
        logger.info(f"Scheduler started. Task will run with interval: {kwargs}")
        
    if trigger_args["trigger"] == "cron":
        scheduler.add_job(fetch_and_save_data, trigger)
    
    scheduler.start()
    
    yield
    
    # Shutdown logic
    scheduler.shutdown()

from routers.debt import router as debt_router
from routers.stability import router as stability_router
from routers.pool import router as pool_router
from routers.auth import router as auth_router
from routers.chains import router as chains_router
from routers.debank import router as debank_router

from fastapi.middleware.cors import CORSMiddleware

import json
from utils import load_chains

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

app = FastAPI(lifespan=lifespan, title="Data Hunt API", description=get_description_with_chains())

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(debt_router)
app.include_router(stability_router)
app.include_router(pool_router)
app.include_router(debank_router)

app.include_router(auth_router)
app.include_router(chains_router)

from routers.health import router as health_router
app.include_router(health_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
