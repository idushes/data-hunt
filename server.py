import os
import uvicorn
from fastapi import FastAPI

from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import DEBANK_ACCESS_KEY, PORT, get_target_ids, get_schedule_time_parts, RUN_ON_STARTUP
from tasks import fetch_and_save_data

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    if not DEBANK_ACCESS_KEY:
        print("DEBANK_ACCESS_KEY is not set in environment variables!")
    
    ids = get_target_ids()
    print(f"Scheduler configured. Target IDs: {ids}")

    if RUN_ON_STARTUP:
        print("RUN_ON_STARTUP is True. Executing fetch task now...")
        await fetch_and_save_data()

    hour, minute = get_schedule_time_parts()
    
    scheduler = AsyncIOScheduler()
    trigger = CronTrigger(hour=hour, minute=minute)
    scheduler.add_job(fetch_and_save_data, trigger)
    scheduler.start()
    
    print(f"Scheduler started. Task will run daily at {hour:02d}:{minute:02d}")
    
    yield
    
    # Shutdown logic
    scheduler.shutdown()

from routers.debt import router as debt_router
from routers.stability import router as stability_router

app = FastAPI(lifespan=lifespan)
app.include_router(debt_router)
app.include_router(stability_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
