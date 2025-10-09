from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG


class RealtimeMetatraderLoad:
    def __init__(self) -> None:
        try:
            self.logger = LoggerConfig.logger_config(
                "Load realtime metatrader gold data"
            )
            self.batch_size_extract = GOLD_DATA_CONFIG["batch_size_extract"]
            self.mongo_config = MongoConfig()
            self.mongo_client = self.mongo_config.get_client()
            self.gold_db = self.mongo_client.get_database(GOLD_DATA_CONFIG["database"])
            self.gold_collection = self.gold_db.get_collection(
                GOLD_DATA_CONFIG["collection"]
            )
            self.logger.info("Successfully to connect MongoDB Config")
        except Exception as e:
            self.logger.error(f"Can not to connect MongoDB Config: {str(e)}")
            raise

    def chunk_data_frame(self, df, chunk_size):
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i : i + chunk_size]

    def realtime_load(self, df):
        self.logger.info("Start load batch realtime metatrader data ...")
        chunk_size = self.batch_size_extract
        batch_count = 0
        for chunk in self.chunk_data_frame(df, chunk_size=chunk_size):
            try:
                chunk_data = chunk.to_dict("records")
                self.gold_collection.create_index(
                    [("date", 1)], unique=True, background=True
                )
                self.gold_collection.insert_many(chunk_data, ordered=False)
                batch_count += 1
                self.logger.info(
                    f"Batch {batch_count} processed: {len(chunk_data)} records"
                )
            except Exception as e:
                self.logger.error(f"Error to load realtime metatrader data: {str(e)}")
        self.logger.info(f"Total batches processed: {batch_count}")
