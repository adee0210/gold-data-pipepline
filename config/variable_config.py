import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB Configuration
MONGO_CONFIG = {
    "port": int(os.getenv("MONGO_PORT", 27017)),
    "host": os.getenv("MONGO_HOST", "localhost"),
    "user": os.getenv("MONGO_USER"),
    "pass": os.getenv("MONGO_PASS"),
    "authSource": os.getenv("MONGO_AUTH", "admin"),
}

# Gold Data Configuration
GOLD_DATA_CONFIG = {
    "database": os.getenv("GOLD_DB_NAME", "gold_db"),
    "collection": os.getenv("GOLD_COLLECTION_NAME", "gold_minute_data"),
    "batch_size_extract": int(os.getenv("BATCH_SIZE", 10000)),
    "metatrader_data_gdrive_url": os.getenv(
        "GDRIVE_URL", "https://drive.google.com/uc?id=1v7HVgXhUmGEUbmbkPxpZ44RiUJH8V3NK"
    ),
    "realtime_interval_seconds": 30,
}

# TradingView Configuration
TRADINGVIEW_CONFIG = {
    "max_retries": int(os.getenv("TV_MAX_RETRIES", 3)),
    "retry_delay": int(os.getenv("TV_RETRY_DELAY", 5)),
    "default_n_bars": int(os.getenv("TV_DEFAULT_N_BARS", 100)),
}
