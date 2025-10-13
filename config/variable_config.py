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
    "historical_data_url_from_kaggle": "novandraanugrah/xauusd-gold-price-historical-data-2004-2024",
    "historical_data_url_from_kaggle_file_path": "XAU_1m_data.csv",
    "batch_size_extract": 10000,
    "metatrader_data_gdrive_url": "https://drive.google.com/uc?id=16FF4z8My2FIQuGhgkHOGuMyrw3uw2eam",
    "metatrader_data_local_path": "data/gold_data_metatrader5.csv",
}
