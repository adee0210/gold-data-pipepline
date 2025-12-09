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
}

# TradingView Configuration
TRADINGVIEW_CONFIG = {
    "symbol": os.getenv("TV_SYMBOL", "XAUUSD"),
    "exchange": os.getenv("TV_EXCHANGE", "OANDA"),
    "interval": "1minute",
    "max_retries": int(os.getenv("TV_MAX_RETRIES", 3)),
    "retry_delay": float(os.getenv("TV_RETRY_DELAY", 2.0)),
    "default_n_bars": int(os.getenv("TV_DEFAULT_BARS", 10)),
    "max_n_bars": int(os.getenv("TV_MAX_BARS", 5000)),
}

# Logging Configuration
LOGGING_CONFIG = {
    "log_file": os.getenv("LOG_FILE", "main.log"),
    "log_level": os.getenv("LOG_LEVEL", "INFO"),
    "max_bytes": int(os.getenv("LOG_MAX_BYTES", 10 * 1024 * 1024)),  # 10MB
    "backup_count": int(os.getenv("LOG_BACKUP_COUNT", 5)),
}

# Market Hours Configuration (Vietnam timezone GMT+7)
MARKET_CONFIG = {
    "open_weekday": 0,  # Monday
    "close_weekday": 5,  # Saturday
    "daily_open_hour": 6,  # 6 AM Vietnam time
    "weekend_close_enabled": True,
}
