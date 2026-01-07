import os
import sys
import signal
import logging
from pathlib import Path

# Thêm thư mục gốc vào sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Tắt logging từ tvDatafeed để không spam console
logging.getLogger("tvDatafeed").setLevel(logging.CRITICAL)
logging.getLogger("tvDatafeed.main").setLevel(logging.CRITICAL)

from config.logger_config import LoggerConfig
from src.pipepline.historical_metatrader_pipepline import HistoricalMetatraderPipepline
from src.pipepline.realtime_metatrader_pipepline import RealtimeMetatraderPipepline


class GoldDataMain:
    def __init__(self):
        self.logger = LoggerConfig.logger_config("Main Gold Data Pipeline")
        self.historical_completed = False
        self.shutdown_requested = False

        # Setup signal handlers cho graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Nhận tín hiệu shutdown: {signum}")
        self.shutdown_requested = True

    def run_historical(self):
        """Chạy pipeline lịch sử (chỉ chạy 1 lần nếu cần)"""
        if self.historical_completed:
            self.logger.info("Pipeline lịch sử đã được chạy, bỏ qua")
            return

        try:
            self.logger.info("=" * 80)
            self.logger.info("BẮT ĐẦU PIPELINE LỊCH SỬ GOLD DATA")
            self.logger.info("=" * 80)

            hist_pipeline = HistoricalMetatraderPipepline()
            hist_pipeline.run()

            self.historical_completed = True
            self.logger.info("=" * 80)
            self.logger.info("HOÀN THÀNH PIPELINE LỊCH SỬ GOLD DATA")
            self.logger.info("=" * 80)
        except Exception as e:
            self.logger.error(f"Lỗi khi chạy pipeline lịch sử: {str(e)}")
            self.logger.exception(e)
            self.historical_completed = True

    def run_realtime(self):
        """Chạy pipeline realtime liên tục"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("BẮT ĐẦU PIPELINE REALTIME GOLD DATA")
            self.logger.info("=" * 80)

            rt_pipeline = RealtimeMetatraderPipepline()
            rt_pipeline.run_realtime()

            self.logger.info("=" * 80)
            self.logger.info("DỪNG PIPELINE REALTIME GOLD DATA")
            self.logger.info("=" * 80)
        except Exception as e:
            self.logger.error(f"Lỗi nghiêm trọng khi chạy pipeline realtime: {str(e)}")
            self.logger.exception(e)

    def run(self):
        """Chạy toàn bộ ứng dụng"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("KHỞI ĐỘNG ỨNG DỤNG GOLD DATA PIPELINE")
            self.logger.info("=" * 80)

            # Bước 1: Chạy pipeline lịch sử (chỉ 1 lần)
            self.run_historical()

            # Bước 2: Chạy pipeline realtime liên tục
            self.run_realtime()

        except KeyboardInterrupt:
            self.logger.info("Nhận tín hiệu dừng từ người dùng - Thoát ứng dụng")
        except Exception as e:
            self.logger.error(f"Lỗi nghiêm trọng: {str(e)}")
            self.logger.exception(e)
            raise
        finally:
            self.logger.info("=" * 80)
            self.logger.info("THOÁT ỨNG DỤNG GOLD DATA PIPELINE")
            self.logger.info("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("KHỞI ĐỘNG GOLD DATA PIPELINE")
    print("=" * 80)
    print(f"Log file: logs/main.log")
    print("=" * 80)
    print("Theo dõi log realtime: tail -f logs/main.log")
    print("=" * 80)
    print()

    try:
        main_app = GoldDataMain()
        main_app.run()
    except KeyboardInterrupt:
        print("\n⛔ Dừng ứng dụng bởi người dùng (Ctrl+C)")
        logger = LoggerConfig.logger_config("Main Gold Data Pipeline")
        logger.info("Ứng dụng dừng bởi người dùng")
    except Exception as e:
        print(f"\n❌ Lỗi: {str(e)}")
        logger = LoggerConfig.logger_config("Main Gold Data Pipeline")
        logger.error(f"Ứng dụng bị crash: {str(e)}")
        logger.exception(e)
