import asyncio
import os
import sys
import time
import signal
import logging
import psutil
from datetime import datetime, timedelta
from pathlib import Path


# Thêm thư mục gốc vào sys.path (để import config và src)
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# File lưu PID
PID_FILE = Path(__file__).parent / "gold_data_project.pid"

# Tắt logging từ tvDatafeed để không spam console
logging.getLogger("tvDatafeed").setLevel(logging.CRITICAL)
logging.getLogger("tvDatafeed.main").setLevel(logging.CRITICAL)

from config.logger_config import LoggerConfig
from src.pipepline.historical_metatrader_pipepline import HistoricalMetatraderPipepline
from src.pipepline.realtime_metatrader_pipepline import RealtimeMetatraderPipepline
from src.etl.load.historical_metatrader_load import HistoricalMetatraderLoad

from config.variable_config import GOLD_DATA_CONFIG


class GoldDataMain:
    def __init__(self, skip_existing=True):
        self.logger = LoggerConfig.logger_config("Main Gold Data Pipeline")
        self.historical_completed = False
        self.skip_existing = skip_existing  # Chỉ trích xuất dữ liệu còn thiếu
        self.symbols = GOLD_DATA_CONFIG.get("symbols", ["XAUUSD"])  # Gold symbols
        self.loader = HistoricalMetatraderLoad()

        # Setup signal handlers cho graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        self.shutdown_requested = False

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Nhận tín hiệu shutdown: {signum}")
        self.shutdown_requested = True

    def _check_if_historical_needed(self):
        """
        Kiểm tra xem có cần chạy historical extract không
        Logic: Chỉ chạy historical extract nếu CHƯA CÓ DATA
        - Đã có data → BỎ QUA historical (dùng realtime update)
        - Chưa có data → Chạy historical lần đầu
        Returns: (needed, reason)
            - needed: True nếu cần chạy historical
            - reason: Lý do cần/không cần chạy
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("KIỂM TRA DỮ LIỆU TRONG DATABASE")
            self.logger.info("=" * 80)

            symbols_without_data = []
            symbols_with_data = []

            for symbol in self.symbols:
                # Đếm số lượng records của symbol
                count = self.loader.gold_collection.count_documents({"symbol": symbol})

                if count == 0:
                    self.logger.warning(
                        f"[{symbol}] Chưa có dữ liệu trong DB (0 records)"
                    )
                    symbols_without_data.append(symbol)
                else:
                    # Lấy thông tin data mới nhất
                    latest_record = self.loader.gold_collection.find_one(
                        {"symbol": symbol}, sort=[("datetime", -1)]
                    )
                    if latest_record and "datetime" in latest_record:
                        latest_dt_str = latest_record["datetime"]
                        latest_dt = datetime.fromisoformat(
                            latest_dt_str.replace("Z", "+00:00")
                        )

                        self.logger.info(
                            f"[{symbol}]  Đã có {count:,} records, mới nhất: {latest_dt.strftime('%Y-%m-%d %H:%M')}"
                        )
                    else:
                        self.logger.info(
                            f"[{symbol}]  Đã có {count:,} records trong DB"
                        )
                    symbols_with_data.append(symbol)

            self.logger.info("=" * 80)
            if symbols_without_data:
                reason = (
                    f"Cần chạy historical cho {len(symbols_without_data)}/{len(self.symbols)} symbols chưa có data: "
                    f"{', '.join(symbols_without_data[:5])}"
                    + (
                        f" và {len(symbols_without_data) - 5} symbols khác"
                        if len(symbols_without_data) > 5
                        else ""
                    )
                )
                self.logger.info(f"KẾT LUẬN: {reason}")
                self.logger.info("SẼ CHẠY HISTORICAL EXTRACT")
                self.logger.info("=" * 80)
                return True, reason
            else:
                reason = (
                    f"Tất cả {len(self.symbols)} symbols đã có dữ liệu trong DB\n"
                    f"Realtime extract sẽ tự động cập nhật dữ liệu mới"
                )
                self.logger.info(f"KẾT LUẬN: {reason}")
                self.logger.info("BỎ QUA HISTORICAL EXTRACT - CHỈ CHẠY REALTIME")
                self.logger.info("=" * 80)
                return False, reason

        except Exception as e:
            self.logger.error(f"Lỗi khi kiểm tra data: {str(e)}")
            self.logger.exception(e)
            # Nếu lỗi, để an toàn thì KHÔNG chạy historical (tránh tốn RAM)
            return False, "Lỗi khi kiểm tra → Bỏ qua historical để an toàn"

    def run_historical(self):
        """Chạy pipeline lịch sử (chỉ chạy 1 lần nếu cần) - với resilient error handling"""
        if self.historical_completed:
            self.logger.info("Pipeline lịch sử đã được chạy, bỏ qua")
            return

        # Kiểm tra xem có cần chạy historical extract không
        try:
            needed, reason = self._check_if_historical_needed()
        except Exception as e:
            self.logger.error(
                f"Lỗi khi kiểm tra historical data, bỏ qua historical: {str(e)}"
            )
            self.historical_completed = True
            return

        if not needed:
            self.logger.info(f"BỎ QUA HISTORICAL EXTRACT: {reason}")
            self.historical_completed = True
            return

        try:
            self.logger.info("=" * 80)
            self.logger.info("BẮT ĐẦU PIPELINE LỊCH SỬ GOLD DATA")
            self.logger.info(f"Lý do: {reason}")
            if self.skip_existing:
                self.logger.info("Chế độ: Chỉ trích xuất dữ liệu còn thiếu")
            else:
                self.logger.info("Chế độ: Trích xuất toàn bộ lại")
            self.logger.info("=" * 80)

            historical_pipeline = HistoricalMetatraderPipepline()
            # Chạy pipeline với error handling
            try:
                historical_pipeline.run()
            except Exception as e:
                self.logger.error(
                    f"Lỗi trong historical pipeline, nhưng sẽ tiếp tục realtime: {str(e)}"
                )

            self.historical_completed = True
            self.logger.info("=" * 80)
            self.logger.info("HOÀN THÀNH PIPELINE LỊCH SỬ GOLD DATA")
            self.logger.info("=" * 80)
        except Exception as e:
            # Không raise, chỉ log để realtime vẫn chạy được
            self.logger.error(f"Lỗi khi chạy pipeline lịch sử: {str(e)}")
            self.historical_completed = True  # Đánh dấu hoàn thành để tiếp tục realtime

    def run_realtime(self):
        """Chạy pipeline realtime liên tục - với resilient error handling"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("BẮT ĐẦU PIPELINE REALTIME GOLD DATA")
            self.logger.info("=" * 80)

            realtime_pipeline = RealtimeMetatraderPipepline()

            # Bắt đầu vòng lặp realtime liên tục
            self.logger.info("=" * 80)
            self.logger.info("BẮT ĐẦU VÒNG LẶP REALTIME (CẬP NHẬT MỖI 30 GIÂY)")
            self.logger.info("=" * 80)

            # Chạy pipeline realtime (sẽ chạy liên tục bên trong)
            realtime_pipeline.run_realtime()

            self.logger.info("=" * 80)
            self.logger.info("DỪNG PIPELINE REALTIME GOLD DATA")
            self.logger.info("=" * 80)

        except Exception as e:
            self.logger.error(f"Lỗi nghiêm trọng khi chạy pipeline realtime: {str(e)}")

    def run(self):
        """Chạy toàn bộ ứng dụng"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("KHỞI ĐỘNG ỨNG DỤNG GOLD DATA PIPELINE")
            self.logger.info(f"Cấu hình: Background processing")
            if self.skip_existing:
                self.logger.info(
                    "Chế độ: Tự động kiểm tra và chỉ trích xuất dữ liệu còn thiếu"
                )
            else:
                self.logger.info("Chế độ: Trích xuất toàn bộ lại từ đầu")
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
            # Re-raise để trigger restart loop
            raise
        finally:
            self.logger.info("=" * 80)
            self.logger.info("THOÁT ỨNG DỤNG GOLD DATA PIPELINE")
            self.logger.info("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Gold Data Pipeline")
    parser.add_argument(
        "action",
        nargs="?",
        default="start",
        choices=["start", "stop", "restart", "status"],
        help="Action to perform: start, stop, restart, or status",
    )
    args = parser.parse_args()

    def get_pid():
        """Đọc PID từ file"""
        if PID_FILE.exists():
            try:
                return int(PID_FILE.read_text().strip())
            except:
                return None
        return None

    def save_pid():
        """Lưu PID hiện tại vào file"""
        PID_FILE.write_text(str(os.getpid()))

    def is_process_running(pid):
        """Kiểm tra process có đang chạy không"""
        if pid is None:
            return False
        try:
            process = psutil.Process(pid)
            return process.is_running()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    def stop_process():
        """Dừng process đang chạy"""
        pid = get_pid()
        if not pid:
            print("Khong tim thay PID file. Process co the da dung.")
            return False

        if not is_process_running(pid):
            print(f"Process {pid} khong con chay.")
            PID_FILE.unlink(missing_ok=True)
            return False

        try:
            print(f"Dang dung process {pid}...")
            os.kill(pid, signal.SIGTERM)

            # Đợi process dừng (tối đa 10 giây)
            for i in range(10):
                if not is_process_running(pid):
                    print(f"Process {pid} da dung thanh cong")
                    PID_FILE.unlink(missing_ok=True)
                    return True
                time.sleep(1)

            # Nếu chưa dừng, force kill
            print(f"Process {pid} chua dung, force killing...")
            os.kill(pid, signal.SIGKILL)
            time.sleep(1)
            PID_FILE.unlink(missing_ok=True)
            print(f"Process {pid} da bi force kill")
            return True

        except Exception as e:
            print(f"❌ Lỗi khi dừng process: {e}")
            return False

    def show_status():
        """Hiển thị trạng thái process"""
        pid = get_pid()
        if not pid:
            print("❌ Pipeline KHÔNG chạy (không tìm thấy PID file)")
            return

        if is_process_running(pid):
            try:
                process = psutil.Process(pid)
                print("=" * 80)
                print(f"Pipeline DANG CHAY")
                print(f"   PID: {pid}")
                print(f"   CPU: {process.cpu_percent(interval=0.1):.1f}%")
                print(f"   Memory: {process.memory_info().rss / 1024 / 1024:.1f} MB")
                print(
                    f"   Start time: {datetime.fromtimestamp(process.create_time()).strftime('%Y-%m-%d %H:%M:%S')}"
                )
                print("=" * 80)
                print(f"Xem log: tail -f logs/main.log")
                print(f"Dung: {sys.argv[0]} stop")
                print("=" * 80)
            except Exception as e:
                print(f"Loi khi lay thong tin process: {e}")
        else:
            print(f"❌ Pipeline KHÔNG chạy (PID {pid} không tồn tại)")
            PID_FILE.unlink(missing_ok=True)

    # Xử lý các action
    if args.action == "status":
        show_status()
        sys.exit(0)

    elif args.action == "stop":
        if stop_process():
            sys.exit(0)
        else:
            sys.exit(1)

    elif args.action == "restart":
        print("Dang restart pipeline...")
        stop_process()
        time.sleep(2)
        print("Dang khoi dong lai...")
        # Tiếp tục xuống dưới để start

    # Action: start hoặc restart (sau khi stop)
    existing_pid = get_pid()
    if existing_pid and is_process_running(existing_pid):
        print(f"Pipeline da chay voi PID {existing_pid}")
        print(f"💡 Dùng '{sys.argv[0]} restart' để khởi động lại")
        print(f"💡 Dùng '{sys.argv[0]} status' để xem trạng thái")
        sys.exit(1)

    # Fork process để chạy background
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process
            save_pid()
            print("=" * 80)
            print("KHOI DONG GOLD DATA PIPELINE THANH CONG")
            print("=" * 80)
            print(f"PID: {pid}")
            print(f"Log file: logs/main.log")
            print(f"Interval: 30 giay")
            print(f"Symbols: {', '.join(GOLD_DATA_CONFIG.get('symbols', ['XAUUSD']))}")
            print("=" * 80)
            print(f"Xem log realtime: tail -f logs/main.log")
            print(f"Kiem tra trang thai: {sys.argv[0]} status")
            print(f"Dung pipeline: {sys.argv[0]} stop")
            print(f"Khoi dong lai: {sys.argv[0]} restart")
            print("=" * 80)
            sys.exit(0)
    except OSError as e:
        print(f"❌ Lỗi khi fork process: {e}")
        sys.exit(1)

    # Child process - chạy pipeline
    # Detach from terminal
    os.setsid()

    # Redirect stdout/stderr to log file
    sys.stdout.flush()
    sys.stderr.flush()

    # Chạy pipeline
    try:
        main_app = GoldDataMain(skip_existing=True)
        main_app.run()
    except KeyboardInterrupt:
        logger = LoggerConfig.logger_config("Main Gold Data Pipeline")
        logger.info("Ứng dụng dừng bởi người dùng")
    except Exception as e:
        logger = LoggerConfig.logger_config("Main Gold Data Pipeline")
        logger.error(f"Ứng dụng bị crash: {str(e)}")
        logger.exception(e)
    finally:
        # Xóa PID file khi thoát
        PID_FILE.unlink(missing_ok=True)
