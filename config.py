import os
from dotenv import load_dotenv

# Load .env file if exists
load_dotenv()

# Configuration
COINMARKETCAP_API_KEY = os.environ.get("COINMARKETCAP_API_KEY")
COINMARKETCAP_BASE_URL = os.environ.get(
    "COINMARKETCAP_BASE_URL", "https://pro-api.coinmarketcap.com"
)
COINMARKETCAP_CACHE_TTL_SECONDS = min(
    int(os.environ.get("COINMARKETCAP_CACHE_TTL_SECONDS", 3600)), 3600
)
CSV_CACHE_TTL_SECONDS = max(60, int(os.environ.get("CSV_CACHE_TTL_SECONDS", 60)))
CSV_CACHE_MAX_ENTRIES = max(1, int(os.environ.get("CSV_CACHE_MAX_ENTRIES", 256)))
CSV_CACHE_FLIGHT_TIMEOUT_SECONDS = max(
    30, int(os.environ.get("CSV_CACHE_FLIGHT_TIMEOUT_SECONDS", 180))
)
CSV_CACHE_STALE_TTL_SECONDS = max(
    3600, int(os.environ.get("CSV_CACHE_STALE_TTL_SECONDS", 86400))
)
CSV_CACHE_REFRESH_TIMEOUT_SECONDS = max(
    1, int(os.environ.get("CSV_CACHE_REFRESH_TIMEOUT_SECONDS", 8))
)
SHEETS_REFRESH_ENABLED = os.environ.get(
    "SHEETS_REFRESH_ENABLED", "true"
).lower() not in {"0", "false", "no"}
SHEETS_REFRESH_DELAY_SECONDS = max(
    60, int(os.environ.get("SHEETS_REFRESH_DELAY_SECONDS", 59 * 60))
)
SHEETS_REFRESH_POLL_SECONDS = max(
    1, int(os.environ.get("SHEETS_REFRESH_POLL_SECONDS", 5))
)
REDIS_URL = os.environ.get("REDIS_URL")
OUTBOUND_QUEUE_ENABLED = os.environ.get(
    "OUTBOUND_QUEUE_ENABLED", "true"
).lower() not in {"0", "false", "no"}
OUTBOUND_QUEUE_MAX_WAIT_SECONDS = max(
    1, int(os.environ.get("OUTBOUND_QUEUE_MAX_WAIT_SECONDS", 120))
)
OUTBOUND_QUEUE_429_RETRIES = max(
    0, min(5, int(os.environ.get("OUTBOUND_QUEUE_429_RETRIES", 2)))
)
OUTBOUND_API_LIMITS_JSON = os.environ.get("OUTBOUND_API_LIMITS_JSON", "")
PORT = int(os.environ.get("PORT", 8111))
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./data.db")
SECRET_KEY = os.environ.get(
    "SECRET_KEY", "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(
    os.environ.get("ACCESS_TOKEN_EXPIRE_MINUTES", 60 * 24)
)
VALUE_RATE_LIMIT_WINDOW_SECONDS = max(
    1, int(os.environ.get("VALUE_RATE_LIMIT_WINDOW_SECONDS", 60))
)
VALUE_RATE_LIMIT_AUTHENTICATED = max(
    1, int(os.environ.get("VALUE_RATE_LIMIT_AUTHENTICATED", 120))
)
FEATURE_REQUEST_ADMIN_ADDRESSES = frozenset(
    address.strip().lower()
    for address in os.environ.get("FEATURE_REQUEST_ADMIN_ADDRESSES", "").split(",")
    if address.strip()
)
