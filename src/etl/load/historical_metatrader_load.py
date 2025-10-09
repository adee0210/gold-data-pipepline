from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG
from pymongo.errors import BulkWriteError


class HistoricalMetatraderLoad:
    def __init__(self) -> None:
        try:
            self.logger = LoggerConfig.logger_config(
                "Load historical metatrader gold data"
            )
            self.batch_size_extract = GOLD_DATA_CONFIG["batch_size_extract"]
            self.mongo_config = MongoConfig()
            self.mongo_client = self.mongo_config.get_client()
            self.gold_db = self.mongo_client.get_database(GOLD_DATA_CONFIG["database"])
            self.gold_collection = self.gold_db.get_collection(
                GOLD_DATA_CONFIG["collection"]
            )
            # Ensure unique index exists (created once)
            try:
                self.gold_collection.create_index(
                    [("date", 1)], unique=True, background=True
                )
            except Exception:
                # non-fatal if index creation fails here; it may already exist
                self.logger.debug("Index creation skipped or failed; continuing")
            self.logger.info("Successfully to connect MongoDB Config")
        except Exception as e:
            self.logger.error(f"Can not to connect MongoDB Config: {str(e)}")
            raise

    def chunk_data_frame(self, metatrader_data_extract, chunk_size):
        for i in range(0, len(metatrader_data_extract), chunk_size):
            yield metatrader_data_extract.iloc[i : i + chunk_size]

    def historical_load(self, metatrader_data_extract):
        self.logger.info("Start load batch historical metatrader data ...")
        chunk_size = self.batch_size_extract
        batch_count = 0
        for chunk in self.chunk_data_frame(
            metatrader_data_extract, chunk_size=chunk_size
        ):
            try:
                chunk_data = chunk.to_dict("records")
                # Use unordered insert to continue past duplicate key errors
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
                # Summarize duplicate key errors to avoid noisy logging and lag
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
                    # Log only first non-duplicate error to avoid flooding logs
                    self.logger.error(
                        f"Non-duplicate write error in batch {batch_count}: {other_errors[0]}"
                    )
            except Exception as e:
                # Unexpected errors should be visible
                self.logger.exception(
                    f"Unexpected error to load historical metatrader data: {str(e)}"
                )
        self.logger.info(f"Total batches processed: {batch_count}")
