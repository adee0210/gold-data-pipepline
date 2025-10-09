from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG
from pymongo.errors import BulkWriteError


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
            try:
                self.gold_collection.create_index(
                    [("datetime", 1)], unique=True, background=True
                )
            except Exception:
                self.logger.debug("Index creation skipped or failed; continuing")
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
                result = self.gold_collection.insert_many(chunk_data, ordered=False)
                inserted = (
                    len(result.inserted_ids)
                    if result and getattr(result, "inserted_ids", None) is not None
                    else 0
                )
                batch_count += 1
                self.logger.info(
                    f"Batch {batch_count} inserted {inserted}/{len(chunk_data)} records"
                )
            except BulkWriteError as bwe:
                details = bwe.details or {}
                nInserted = details.get("nInserted", 0)
                writeErrors = details.get("writeErrors", []) or []
                dup_count = sum(1 for we in writeErrors if we.get("code") == 11000)
                other_errors = [we for we in writeErrors if we.get("code") != 11000]
                batch_count += 1
                self.logger.info(
                    f"Batch {batch_count} partial insert: {nInserted}/{len(chunk_data)} inserted, duplicates: {dup_count}, other write errors: {len(other_errors)}"
                )
                if other_errors:
                    self.logger.error(
                        f"Non-duplicate write error in batch {batch_count}: {other_errors[0]}"
                    )
            except Exception as e:
                self.logger.error(f"Error to load realtime metatrader data: {str(e)}")
        self.logger.info(f"Total batches processed: {batch_count}")

    def upsert_current_minute_candle(self, df):
        """Upsert nến phút hiện tại - update nếu tồn tại, insert nếu chưa có"""
        if df.empty:
            self.logger.warning("No data to upsert")
            return

        self.logger.info("Upserting current minute candle...")

        for _, row in df.iterrows():
            candle_data = row.to_dict()
            datetime_key = candle_data["datetime"]

            try:
                # Sử dụng upsert: update nếu tồn tại, insert nếu chưa có
                result = self.gold_collection.update_one(
                    {"datetime": datetime_key},  # Filter theo datetime
                    {"$set": candle_data},  # Update toàn bộ document
                    upsert=True,  # Insert nếu không tìm thấy
                )

                if result.upserted_id:
                    self.logger.info(
                        f"Inserted new candle for {datetime_key}: close={candle_data.get('close')}, volume={candle_data.get('volume')}"
                    )
                elif result.modified_count > 0:
                    self.logger.info(
                        f"Updated candle for {datetime_key}: close={candle_data.get('close')}, volume={candle_data.get('volume')}"
                    )
                else:
                    self.logger.debug(f"No changes for candle {datetime_key}")

            except Exception as e:
                self.logger.error(
                    f"Error upserting candle for {datetime_key}: {str(e)}"
                )
