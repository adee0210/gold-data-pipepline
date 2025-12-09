import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.etl.extract.historical_metatrader_extract import HistoricalMetatraderExtract
from src.etl.load.historical_metatrader_load import HistoricalMetatraderLoad
from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG


class HistoricalMetatraderPipepline:
    def __init__(self):
        self.logger = LoggerConfig.logger_config("Historical Pipeline")
        self.extractor = HistoricalMetatraderExtract()
        self.loader = HistoricalMetatraderLoad()

        # MongoDB connection để kiểm tra data
        self.mongo_config = MongoConfig()
        self.mongo_client = self.mongo_config.get_client()
        self.gold_db = self.mongo_client.get_database(GOLD_DATA_CONFIG["database"])
        self.gold_collection = self.gold_db.get_collection(
            GOLD_DATA_CONFIG["collection"]
        )

    def has_data(self):
        """Kiểm tra xem database đã có dữ liệu chưa"""
        count = self.gold_collection.count_documents({})
        return count > 0

    def run(self):
        """Chỉ chạy historical extract nếu chưa có dữ liệu"""
        if self.has_data():
            self.logger.info("Database đã có dữ liệu, bỏ qua historical extract")
            return

        self.logger.info("Database chưa có dữ liệu, bắt đầu chạy historical extract")
        # Extract dữ liệu
        metatrader_data = self.extractor.historical_extract()
        # Load dữ liệu vào MongoDB
        self.loader.historical_load(metatrader_data)


if __name__ == "__main__":
    pipepline = HistoricalMetatraderPipepline()
    pipepline.run()
