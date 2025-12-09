import os
from typing import List
from dotenv import load_dotenv

# Load .env file if exists
load_dotenv()

# Configuration
DEBANK_ACCESS_KEY = os.environ.get("DEBANK_ACCESS_KEY")
SCHEDULE_TIME = os.environ.get("SCHEDULE_TIME", "00:00")
PORT = int(os.environ.get("PORT", 8111))
RUN_ON_STARTUP = os.environ.get("RUN_ON_STARTUP", "false").lower() == "true"

def get_target_ids() -> List[str]:
    """Scans environment variables for keys starting with TARGET_ID_"""
    ids = []
    for key, value in os.environ.items():
        if key.startswith("TARGET_ID_"):
            ids.append(value)
    return ids

def get_schedule_time_parts():
    """Parses SCHEDULE_TIME into (hour, minute) tuple"""
    try:
        hour, minute = map(int, SCHEDULE_TIME.split(":"))
        return hour, minute
    except ValueError:
        print(f"Invalid SCHEDULE_TIME format '{SCHEDULE_TIME}', defaulting to 00:00")
        return 0, 0
