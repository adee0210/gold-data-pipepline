import sys
import os
import schedule
import time
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.etl.extract.realtime_metatrader_extract import RealtimeMetatraderExtract
from src.etl.load.realtime_metatrader_load import RealtimeMetatraderLoad
from src.etl.load.historical_metatrader_load import HistoricalMetatraderLoad
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG


class RealtimeMetatraderPipepline:
    def __init__(self):
        self.extractor = RealtimeMetatraderExtract()
        self.loader = RealtimeMetatraderLoad()
        self.historical_loader = HistoricalMetatraderLoad()

        # Kết nối MongoDB để kiểm tra dữ liệu
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

    def upsert_recent_candles(self):
        """Upsert 10 nến gần nhất mỗi 10 giây"""
        try:
            df = self.extractor.get_recent_candles(n_candles=10)
            if not df.empty:
                self.loader.upsert_current_minute_candle(
                    df
                )  # Sử dụng logic upsert để cập nhật/chen
                logger.info(f"Đã upsert {len(df)} nến gần nhất theo datetime")
        except Exception as e:
            logger.exception(f"Lỗi trong upsert_recent_candles: {e}")

    def run_realtime(self):
        logger.info("Bắt đầu khởi tạo pipeline thời gian thực...")

        logger.info("Bắt đầu backfill 5000 nến lịch sử")
        metatrader_data = self.extractor.fill_historical_data(n_candles=5000)
        self.loader.upsert_current_minute_candle(
            metatrader_data
        )  # Backfill bằng logic upsert

        # Lên lịch: upsert 10 nến gần nhất mỗi 10 giây
        interval = GOLD_DATA_CONFIG.get("realtime_interval_seconds", 10)
        logger.info(f"Thiết lập lịch cho các thao tác upsert mỗi {interval} giây...")
        schedule.every(interval).seconds.do(self.upsert_recent_candles)

        logger.info("Pipeline thời gian thực đã bắt đầu:")
        logger.info(f"- Mỗi {interval} giây: Upsert 10 nến gần nhất")
        logger.info("Nhấn Ctrl+C để dừng.")

        logger.info("Pipeline thời gian thực đã bắt đầu thành công")
        logger.debug("Hoàn thành ghi log pipeline thời gian thực")
        consecutive_errors = 0
        max_consecutive_errors = 10  # Khởi động lại sau 10 lỗi liên tiếp
        loop_count = 0

        try:
            while True:
                loop_count += 1

                try:
                    schedule.run_pending()
                    consecutive_errors = 0  # Đặt lại bộ đếm khi thành công
                    time.sleep(1)
                except KeyboardInterrupt:
                    logger.info("Nhận tín hiệu tắt. Thoát...")
                    sys.exit(0)
                except Exception as e:
                    consecutive_errors += 1
                    logger.exception(
                        f"Lỗi không mong muốn trong vòng lặp chính (lỗi {consecutive_errors}/{max_consecutive_errors}): {e}"
                    )

                    if consecutive_errors >= max_consecutive_errors:
                        logger.warning(
                            f"Quá nhiều lỗi liên tiếp ({consecutive_errors}), đang khởi động lại pipeline..."
                        )
                        # Đặt lại lịch và khởi động lại
                        schedule.clear()
                        consecutive_errors = 0
                        schedule.every(interval).seconds.do(self.upsert_recent_candles)
                        # Đảm bảo không có lịch trùng lặp
                        schedule.clear(self.upsert_recent_candles)
                        schedule.every(interval).seconds.do(self.upsert_recent_candles)
                        logger.info("Pipeline đã khởi động lại sau lỗi")
                    else:
                        time.sleep(5)  # Chờ 5 giây trước khi tiếp tục
        except KeyboardInterrupt:
            logger.info("Nhận tín hiệu tắt. Thoát...")
            sys.exit(0)


if __name__ == "__main__":
    pipeline = RealtimeMetatraderPipepline()
    pipeline.run_realtime()
