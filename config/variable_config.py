import os
from dotenv import load_dotenv

load_dotenv()

# Cấu hình MongoDB
MONGO_CONFIG = {
    "port": int(os.getenv("MONGO_PORT", 27017)),
    "host": os.getenv("MONGO_HOST", "localhost"),
    "user": os.getenv("MONGO_USER"),
    "pass": os.getenv("MONGO_PASS"),
    "authSource": os.getenv("MONGO_AUTH", "admin"),
}

# Cấu hình dữ liệu vàng (Gold Data)
GOLD_DATA_CONFIG = {
    "database": os.getenv("GOLD_DB_NAME", "gold_db"),
    "collection": os.getenv("GOLD_COLLECTION_NAME", "gold_minute_data"),
    "batch_size_extract": int(os.getenv("BATCH_SIZE", 10000)),
    "metatrader_data_gdrive_url": os.getenv(
        "GDRIVE_URL", "https://drive.google.com/uc?id=1v7HVgXhUmGEUbmbkPxpZ44RiUJH8V3NK"
    ),
    "realtime_interval_seconds": 30,
}

# Cấu hình TradingView
TRADINGVIEW_CONFIG = {
    "max_retries": int(os.getenv("TV_MAX_RETRIES", 3)),
    "retry_delay": int(os.getenv("TV_RETRY_DELAY", 5)),
    "default_n_bars": int(os.getenv("TV_DEFAULT_N_BARS", 100)),
}

# Cấu hình Logging (ghi log)
LOGGING_CONFIG = {
    "log_file": "main.log",
    "log_level": "INFO",
    "max_bytes": 10 * 1024 * 1024,  # 10MB
    "backup_count": 5,
}
