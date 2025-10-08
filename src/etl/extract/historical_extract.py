from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG


class HistoricalExtract:
    def __init__(self) -> None:
        try:
            self.logger = LoggerConfig.logger_config("Extract Historical gold data")
            self.mongo_config = MongoConfig()
            self.mongo_client = self.mongo_config.get_client()
            self.gold_db = self.mongo_client.get_database(GOLD_DATA_CONFIG["database"])

            self.gold_collection = self.gold_db.get_collection(
                GOLD_DATA_CONFIG["collection"]
            )
            self.historical_data_url_from_kaggle = GOLD_DATA_CONFIG[
                "historical_data_url_from_kaggle"
            ]
            self.historical_data_url_from_kaggle_file_path = GOLD_DATA_CONFIG["historical_data_url_from_kaggle_file_path"]
            self.logger.info("Successfully to connect MongoDB config")
        except Exception as e:
            self.logger.error(f"Error to connect MongoDB config: {str(e)}")
    
    def historical_extract(self):



        
