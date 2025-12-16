from pymongo import MongoClient
from config.variable_config import MONGO_CONFIG
import logging

logger = logging.getLogger(__name__)


class MongoConfig:
    _instance = None

    def _init_config(self):
        self._config = {
            "host": MONGO_CONFIG.get("host", "localhost"),
            "port": int(MONGO_CONFIG.get("port", 27017)),
            "username": MONGO_CONFIG.get("user"),
            "password": MONGO_CONFIG.get("pass"),
            "authSource": MONGO_CONFIG.get("authSource", "admin"),
            "serverSelectionTimeoutMS": 5000,  # Timeout 5s
            "connectTimeoutMS": 5000,
            "socketTimeoutMS": 5000,
        }

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoConfig, cls).__new__(cls)
            cls._instance._init_config()
            cls._instance._client = None
        return cls._instance

    @property
    def get_config(self):
        return self._config

    def get_client(self):
        """Lazy connection: tạo client mới nếu chưa có hoặc đã bị đóng"""
        if self._client is None:
            try:
                self._client = MongoClient(**self._config)
                # Test connection
                self._client.admin.command("ping")
                logger.info("MongoDB connected successfully")
            except Exception as e:
                logger.error(f"Failed to connect to MongoDB: {e}")
                self._client = None
        return self._client

    def reset_client(self):
        """Reset client để trigger reconnect trong lần gọi tiếp theo"""
        if self._client:
            try:
                self._client.close()
            except:
                pass
        self._client = None
        logger.info("MongoDB client reset, will reconnect on next access")
