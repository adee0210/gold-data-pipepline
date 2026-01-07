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
from config.logger_config import LoggerConfig

import logging

logger = LoggerConfig.logger_config(__name__)


class RealtimeMetatraderPipepline:
    def __init__(self):
        self.extractor = RealtimeMetatraderExtract()
        self.loader = RealtimeMetatraderLoad()
        self.historical_loader = HistoricalMetatraderLoad()

        # Kết nối MongoDB để kiểm tra dữ liệu
        self.mongo_config = MongoConfig()
        self.mongo_client = self.mongo_config.get_client()
        if self.mongo_client is None:
            raise ConnectionError("Không thể kết nối MongoDB")
        self.gold_db = self.mongo_client.get_database(GOLD_DATA_CONFIG["database"])
        self.gold_collection = self.gold_db.get_collection(
            GOLD_DATA_CONFIG["collection"]
        )

    def has_data(self):
        """Kiểm tra xem database đã có dữ liệu chưa"""
        count = self.gold_collection.count_documents({})
        return count > 0

    def upsert_recent_candles(self):
        """Upsert 10 nến gần nhất mỗi 10 giây - KHÔNG BAO GIỜ CRASH"""
        try:
            df = self.extractor.get_recent_candles(n_candles=10)
            if not df.empty:
                self.loader.upsert_current_minute_candle(
                    df
                )  # Sử dụng logic upsert để cập nhật/chen
                logger.info(f"Đã upsert {len(df)} nến gần nhất theo datetime")
            else:
                logger.warning("Không có dữ liệu để upsert, sẽ thử lại ở lần tiếp theo")
        except KeyboardInterrupt:
            raise  # Cho phép dừng bằng Ctrl+C
        except Exception as e:
            # LOG LỖI NHƯNG KHÔNG BAO GIỜ RAISE - để vòng lặp tiếp tục
            logger.error(
                f"Lỗi trong upsert_recent_candles (sẽ bỏ qua và tiếp tục): {e}"
            )
            logger.exception(e)

    def run_realtime(self):
        logger.info("Bắt đầu khởi tạo pipeline thời gian thực...")

        # Luôn backfill 5000 nến lịch sử
        try:
            logger.info("Bắt đầu backfill 5000 nến lịch sử")
            metatrader_data = self.extractor.fill_historical_data(n_candles=5000)
            if not metatrader_data.empty:
                self.loader.upsert_current_minute_candle(
                    metatrader_data
                )  # Backfill bằng logic upsert
                logger.info("Backfill lịch sử hoàn thành")
            else:
                logger.warning("Không có dữ liệu backfill, bỏ qua")
        except Exception as e:
            logger.error(f"Lỗi backfill lịch sử: {e}, bỏ qua và tiếp tục với realtime")
            logger.exception(e)

        # Lên lịch: upsert 10 nến gần nhất mỗi X giây
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
        last_log_time = time.time()
        log_interval = 300  # Log heartbeat mỗi 5 phút

        logger.info("=" * 80)
        logger.info("BẮT ĐẦU VÒNG LẶP REALTIME VÔ HẠN")
        logger.info("Pipeline sẽ chạy liên tục và KHÔNG BAO GIỜ DỪNG trừ khi Ctrl+C")
        logger.info("=" * 80)

        try:
            while True:
                loop_count += 1

                try:
                    schedule.run_pending()
                    consecutive_errors = 0  # Đặt lại bộ đếm khi thành công

                    # Log heartbeat định kỳ
                    current_time = time.time()
                    if current_time - last_log_time >= log_interval:
                        logger.info(
                            f"[HEARTBEAT] Pipeline đang chạy tốt. Loop count: {loop_count:,}"
                        )
                        last_log_time = current_time

                    time.sleep(1)

                except KeyboardInterrupt:
                    logger.info("Nhận tín hiệu tắt. Thoát...")
                    sys.exit(0)
                except Exception as e:
                    consecutive_errors += 1
                    logger.error("=" * 80)
                    logger.error(
                        f"LỖI TRONG VÒNG LẶP CHÍNH (Lỗi {consecutive_errors}/{max_consecutive_errors})"
                    )
                    logger.error(f"Lỗi: {str(e)}")
                    logger.error("Vòng lặp SẼ TIẾP TỤC...")
                    logger.error("=" * 80)
                    logger.exception(e)

                    if consecutive_errors >= max_consecutive_errors:
                        logger.warning("=" * 80)
                        logger.warning(
                            f"QUÁ NHIỀU LỖI LIÊN TIẾP ({consecutive_errors})"
                        )
                        logger.warning("Đang RESET pipeline và khởi động lại...")
                        logger.warning("=" * 80)
                        # Đặt lại lịch và khởi động lại
                        schedule.clear()
                        consecutive_errors = 0
                        schedule.every(interval).seconds.do(self.upsert_recent_candles)
                        logger.info("Pipeline đã RESET và khởi động lại thành công")
                        logger.info("Tiếp tục vòng lặp...")
                    else:
                        time.sleep(5)  # Chờ 5 giây trước khi tiếp tục

        except KeyboardInterrupt:
            logger.info("Nhận tín hiệu tắt từ người dùng. Thoát ứng dụng...")
            sys.exit(0)
        except Exception as e:
            # Nếu có lỗi ngoài vòng lặp, log nhưng KHÔNG crash
            logger.error("=" * 80)
            logger.error("LỖI NGHIÊM TRỌNG BÊN NGOÀI VÒNG LẶP CHÍNH")
            logger.error(f"Lỗi: {str(e)}")
            logger.error("=" * 80)
            logger.exception(e)
            raise  # Re-raise để main.py có thể restart


if __name__ == "__main__":
    pipeline = RealtimeMetatraderPipepline()
    pipeline.run_realtime()
