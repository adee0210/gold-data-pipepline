import os
from dotenv import load_dotenv

load_dotenv()

MONGO_CONFIG = {
    "port": os.getenv("MONGO_PORT", 27017),
    "host": os.getenv("MONGO_HOST", "localhost"),
    "user": os.getenv("MONGO_USER"),
    "pass": os.getenv("MONGO_PASS"),
    "authSource": os.getenv("MONGO_AUTH", "admin"),
}

GOLD_DATA_CONFIG = {
    "database": "gold_db",
    "collection": "gold_minute_data",
    "batch_size_extract": 10000,
    "metatrader_data_gdrive_url": "https://drive.google.com/uc?id=1v7HVgXhUmGEUbmbkPxpZ44RiUJH8V3NK",
    "metatrader_data_local_path": "data/gold_data_metatrader5.csv",
}

DISCORD_CONFIG = {
    "webhook_url": os.getenv("DISCORD_WEBHOOK_URL", ""),
    "enabled": os.getenv("DISCORD_ALERT_ENABLED", "false").lower() == "true",
}
