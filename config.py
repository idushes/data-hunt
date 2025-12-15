import os
import logging
from typing import List
from dotenv import load_dotenv

# Load .env file if exists
load_dotenv()

# Configuration
DEBANK_ACCESS_KEY = os.environ.get("DEBANK_ACCESS_KEY")
UPDATE_INTERVAL = os.environ.get("UPDATE_INTERVAL", "24h")
PORT = int(os.environ.get("PORT", 8111))
RUN_ON_STARTUP = os.environ.get("RUN_ON_STARTUP", "false").lower() == "true"
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./data.db")
SECRET_KEY = os.environ.get("SECRET_KEY", "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 # 1 day by default


def get_target_ids() -> List[str]:
    """Scans environment variables for keys starting with TARGET_ID_"""
    ids = []
    for key, value in os.environ.items():
        if key.startswith("TARGET_ID_"):
            ids.append(value)
    return ids

def get_scheduler_trigger_args():
    """Parses UPDATE_INTERVAL into scheduler trigger arguments"""
    val = UPDATE_INTERVAL.strip()
    
    # Cron capability (HH:MM)
    if ":" in val:
        try:
            hour, minute = map(int, val.split(":"))
            return {"trigger": "cron", "hour": hour, "minute": minute}
        except ValueError:
            logging.warning(f"Invalid UPDATE_INTERVAL format '{val}', defaulting to 24h")
            return {"trigger": "interval", "hours": 24}

    # Interval capability
    try:
        if val.endswith("m"):
            return {"trigger": "interval", "minutes": int(val[:-1])}
        elif val.endswith("h"):
            return {"trigger": "interval", "hours": int(val[:-1])}
        elif val.endswith("d"):
            return {"trigger": "interval", "days": int(val[:-1])}
    except ValueError:
        pass
    
    logging.warning(f"Invalid UPDATE_INTERVAL format '{val}', defaulting to 24h")
    return {"trigger": "interval", "hours": 24}
