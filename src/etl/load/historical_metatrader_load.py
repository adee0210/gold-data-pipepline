from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG
from pymongo.errors import BulkWriteError


class HistoricalMetatraderLoad:
    def __init__(self) -> None:
        try:
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
                print("Index creation skipped or failed; continuing")
            print("Kết nối MongoDB Config thành công")
        except Exception as e:
            print(f"Không thể kết nối MongoDB Config: {str(e)}")
            raise

    def chunk_data_frame(self, metatrader_data_extract, chunk_size):
        for i in range(0, len(metatrader_data_extract), chunk_size):
            yield metatrader_data_extract.iloc[i : i + chunk_size]

    def historical_load(self, metatrader_data_extract):
        print("Bắt đầu tải batch dữ liệu metatrader lịch sử ...")
        chunk_size = self.batch_size_extract
        batch_count = 0
        for chunk in self.chunk_data_frame(
            metatrader_data_extract, chunk_size=chunk_size
        ):
            try:
                chunk_data = chunk.to_dict("records")
                result = self.gold_collection.insert_many(chunk_data, ordered=False)
                inserted = (
                    len(result.inserted_ids)
                    if result and getattr(result, "inserted_ids", None) is not None
                    else 0
                )
                batch_count += 1
                print(
                    f"Batch {batch_count} inserted {inserted}/{len(chunk_data)} records"
                )
            except BulkWriteError as bwe:
                details = bwe.details or {}
                nInserted = details.get("nInserted", 0)
                writeErrors = details.get("writeErrors", []) or []
                dup_count = sum(1 for we in writeErrors if we.get("code") == 11000)
                other_errors = [we for we in writeErrors if we.get("code") != 11000]
                batch_count += 1
                print(
                    f"Batch {batch_count} partial insert: {nInserted}/{len(chunk_data)} inserted, duplicates: {dup_count}, other write errors: {len(other_errors)}"
                )
                if other_errors:
                    print(
                        f"Non-duplicate write error in batch {batch_count}: {other_errors[0]}"
                    )
            except Exception as e:
                print(f"Unexpected error to load historical metatrader data: {str(e)}")
        print(f"Tổng batch đã xử lý: {batch_count}")
