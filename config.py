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
PORT = int(os.environ.get("PORT", 8111))
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./data.db")
SECRET_KEY = os.environ.get(
    "SECRET_KEY", "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(
    os.environ.get("ACCESS_TOKEN_EXPIRE_MINUTES", 60 * 24)
)
