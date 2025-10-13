import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from src.etl.extract.realtime_metatrader_extract import RealtimeMetatraderExtract
from src.etl.load.realtime_metatrader_load import RealtimeMetatraderLoad
from src.pipepline.realtime_metatrader_pipepline import RealtimeMetatraderPipepline
from config.logger_config import LoggerConfig


def test_previous_minute_update():
    """Test cập nhật nến phút trước"""
    logger = LoggerConfig.logger_config("Test Minute Transition")

    # Khởi tạo các thành phần
    extractor = RealtimeMetatraderExtract()
    loader = RealtimeMetatraderLoad()
    pipeline = RealtimeMetatraderPipepline()

    # 1. Kiểm tra lấy nến phút trước
    logger.info("=== Testing get_previous_minute_final_candle ===")
    previous_candle = extractor.get_previous_minute_final_candle()

    if not previous_candle.empty:
        logger.info(f"Previous minute candle found:")
        logger.info(f"Datetime: {previous_candle.iloc[0]['datetime']}")
        logger.info(f"Open: {previous_candle.iloc[0]['open']}")
        logger.info(f"High: {previous_candle.iloc[0]['high']}")
        logger.info(f"Low: {previous_candle.iloc[0]['low']}")
        logger.info(f"Close: {previous_candle.iloc[0]['close']}")
        logger.info(f"Volume: {previous_candle.iloc[0]['volume']}")

        # 2. Cập nhật nến phút trước
        logger.info("=== Testing update_previous_minute_final_state ===")
        pipeline.update_previous_minute_final_state()
    else:
        logger.warning("No previous minute candle found!")

    # 3. Lấy trạng thái hiện tại
    logger.info("=== Testing current minute handling ===")
    now = datetime.now()
    current_minute = now.replace(second=0, microsecond=0)
    previous_minute = current_minute - timedelta(minutes=1)

    logger.info(f"Current time: {now}")
    logger.info(f"Current minute: {current_minute}")
    logger.info(f"Previous minute: {previous_minute}")
    logger.info(f"Data up-to-date: {extractor.is_data_up_to_date()}")

    current_candle = extractor.get_current_minute_candle()
    if not current_candle.empty:
        logger.info(
            f"Current minute candle found: {current_candle.iloc[0]['datetime']}"
        )
    else:
        logger.info("Current minute candle not yet available")

    logger.info("Test completed!")


if __name__ == "__main__":
    test_previous_minute_update()
