from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG
from pymongo.errors import (
    BulkWriteError,
    ConnectionFailure,
    ServerSelectionTimeoutError,
)


class RealtimeMetatraderLoad:
    def __init__(self) -> None:
        self.batch_size_extract = GOLD_DATA_CONFIG["batch_size_extract"]
        self.mongo_config = MongoConfig()
        self.connection_failures = 0
        self.last_failure_time = None
        self._ensure_connection()

    def _ensure_connection(self):
        """Lazy connection: đảm bảo có kết nối MongoDB hợp lệ"""
        import time
        from datetime import datetime, timedelta

        # Rate limiting: nếu quá nhiều failures trong 1 phút, chờ
        now = datetime.now()
        if self.last_failure_time and (now - self.last_failure_time) < timedelta(
            minutes=1
        ):
            if self.connection_failures >= 5:
                wait_time = min(
                    60, 2 ** (self.connection_failures - 5)
                )  # Exponential backoff
                print(
                    f"Too many connection failures, waiting {wait_time}s before retry"
                )
                time.sleep(wait_time)

        try:
            client = self.mongo_config.get_client()
            if client is None:
                print("MongoDB client is None, will retry on next operation")
                self.connection_failures += 1
                self.last_failure_time = now
                return False

            self.gold_db = client.get_database(GOLD_DATA_CONFIG["database"])
            self.gold_collection = self.gold_db.get_collection(
                GOLD_DATA_CONFIG["collection"]
            )

            print("Kết nối MongoDB đã được xác minh")
            self.connection_failures = 0  # Reset on success
            return True
        except Exception as e:
            print(f"Kết nối MongoDB thất bại: {str(e)}")
            self.mongo_config.reset_client()
            self.connection_failures += 1
            self.last_failure_time = now
            return False

    def chunk_data_frame(self, df, chunk_size):
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i : i + chunk_size]

    def realtime_load(self, df):
        print("Bắt đầu tải batch dữ liệu metatrader thời gian thực ...")

        # Đảm bảo có kết nối trước khi load
        if not self._ensure_connection():
            print("Không thể kết nối MongoDB, bỏ qua batch này")
            return 0

        chunk_size = self.batch_size_extract
        batch_count = 0
        total_inserted = 0

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
                total_inserted += inserted
                print(
                    f"Batch {batch_count} đã chèn {inserted}/{len(chunk_data)} bản ghi"
                )
            except BulkWriteError as bwe:
                details = bwe.details or {}
                nInserted = details.get("nInserted", 0)
                writeErrors = details.get("writeErrors", []) or []
                dup_count = sum(1 for we in writeErrors if we.get("code") == 11000)
                other_errors = [we for we in writeErrors if we.get("code") != 11000]
                batch_count += 1
                total_inserted += nInserted
                print(
                    f"Batch {batch_count} chèn một phần: {nInserted}/{len(chunk_data)} đã chèn, trùng lặp: {dup_count}, lỗi ghi khác: {len(other_errors)}"
                )
                if other_errors:
                    print(
                        f"Lỗi ghi không trùng lặp trong batch {batch_count}: {other_errors[0]}"
                    )
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                # Lỗi kết nối MongoDB - reset client để reconnect lần sau
                print(f"MongoDB connection error in batch {batch_count}: {str(e)}")
                self.mongo_config.reset_client()
                print("Sẽ kết nối lại ở thao tác tiếp theo")
                # Không raise, tiếp tục với batch tiếp theo
            except Exception as e:
                print(f"Lỗi tải dữ liệu metatrader thời gian thực: {str(e)}")
                # Check nếu là lỗi liên quan connection
                if "closed" in str(e).lower() or "connection" in str(e).lower():
                    self.mongo_config.reset_client()
                    print("Connection lost, will reconnect on next operation")

        print(f"Tổng batch đã xử lý: {batch_count}, tổng đã chèn: {total_inserted}")
        return total_inserted

    def upsert_current_minute_candle(self, df):
        """Upsert nến phút hiện tại - update nếu tồn tại, insert nếu chưa có"""
        if df.empty:
            print("Không có dữ liệu để upsert")
            return

        # Đảm bảo có kết nối trước khi upsert
        if not self._ensure_connection():
            print("Không thể kết nối MongoDB, bỏ qua upsert")
            return

        # Ensure upsert logic is clear and concise
        print("Đang upsert nến phút hiện tại theo datetime...")
        for _, row in df.iterrows():
            candle_data = row.to_dict()
            datetime_key = candle_data.get("datetime")

            if not datetime_key:
                print(f"Bỏ qua nến không có datetime: {candle_data}")
                continue

            try:
                result = self.gold_collection.update_one(
                    {"datetime": datetime_key},
                    {"$set": candle_data},
                    upsert=True,
                )

                if result.upserted_id:
                    print(
                        f"Inserted new candle for {datetime_key}: close={candle_data.get('close')}, volume={candle_data.get('volume')}"
                    )
                else:
                    print(
                        f"Updated existing candle for {datetime_key}: close={candle_data.get('close')}, volume={candle_data.get('volume')}"
                    )
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                # Lỗi kết nối MongoDB - reset client để reconnect lần sau
                print(f"MongoDB connection error upserting {datetime_key}: {str(e)}")
                self.mongo_config.reset_client()
                print("Will reconnect on next operation")
            except Exception as e:
                print(f"Lỗi upsert nến cho {datetime_key}: {str(e)}")
                # Check nếu là lỗi liên quan connection
                if "closed" in str(e).lower() or "connection" in str(e).lower():
                    self.mongo_config.reset_client()
                    print("Mất kết nối, sẽ kết nối lại ở thao tác tiếp theo")
