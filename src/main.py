import sys
import os
import signal
import time
import argparse
import subprocess
import logging

# Thêm đường dẫn config vào sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config.logger_config import LoggerConfig

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PID_FILE = os.path.join(SCRIPT_DIR, "../gold_data_project.pid")
LOCK_FILE = os.path.join(SCRIPT_DIR, "../gold_data_project.lock")

# Check and setup virtual environment
VENV_DIR = os.path.join(SCRIPT_DIR, "../.venv")
if not os.path.exists(VENV_DIR):
    try:
        print("Đang tạo môi trường ảo...")
        subprocess.run([sys.executable, "-m", "venv", VENV_DIR], check=True)
        print("Đang cài đặt requirements...")
        venv_python = (
            os.path.join(VENV_DIR, "Scripts", "python.exe")
            if os.name == "nt"
            else os.path.join(VENV_DIR, "bin", "python")
        )
        subprocess.run(
            [venv_python, "-m", "pip", "install", "--upgrade", "pip"], check=True
        )
        subprocess.run(
            [
                venv_python,
                "-m",
                "pip",
                "install",
                "-r",
                os.path.join(os.path.dirname(SCRIPT_DIR), "requirements.txt"),
            ],
            check=True,
        )
        print("Thiết lập môi trường hoàn thành. Vui lòng chạy lại script.")
        sys.exit(0)
    except subprocess.CalledProcessError as e:
        print(f"Lỗi khi thiết lập môi trường ảo: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Lỗi không mong muốn: {e}")
        sys.exit(1)
else:
    print("Môi trường ảo đã tồn tại.")

# Nếu đang chạy với global Python, chuyển sang venv Python
if "venv" not in sys.executable.lower():
    venv_python = (
        os.path.join(VENV_DIR, "Scripts", "python.exe")
        if os.name == "nt"
        else os.path.join(VENV_DIR, "bin", "python")
    )
    if os.path.exists(venv_python):
        print(f"Chuyển sang chạy với Python từ venv: {venv_python}")
        os.execv(venv_python, [venv_python] + sys.argv)
    else:
        print("Không tìm thấy Python trong venv.")
        sys.exit(1)

# Khởi tạo logger
LoggerConfig.logger_config("")
logger = logging.getLogger(__name__)

from src.pipepline.historical_metatrader_pipepline import HistoricalMetatraderPipepline
from src.pipepline.realtime_metatrader_pipepline import RealtimeMetatraderPipepline


def is_process_running(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def main():
    try:
        hist = HistoricalMetatraderPipepline()
        hist.run()

        realtime = RealtimeMetatraderPipepline()

        def handle_sigterm(signum, frame):
            logger.info("Nhận tín hiệu dừng, thoát...")
            raise SystemExit()

        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGINT, handle_sigterm)

        realtime.run_realtime()
    except SystemExit:
        logger.info("Yêu cầu thoát hệ thống")
        raise  # Re-raise để vòng lặp
    except Exception as e:
        logger.exception(f"Lỗi không mong muốn: {e}")
        import traceback

        traceback.print_exc()
        logger.info("Pipeline bị crash, tiếp tục...")


def start():
    if os.path.exists(PID_FILE):
        with open(PID_FILE, "r") as f:
            pid = int(f.read().strip())
        if is_process_running(pid):
            print(f"gold_data_project đã chạy (PID: {pid})")
            return 1
        else:
            print("Đang xóa file PID cũ")
            os.remove(PID_FILE)

    print("Đang khởi động gold_data_project...")
    logger.info("Đang khởi động gold_data_project...")

    import subprocess

    proc = subprocess.Popen([sys.executable, __file__, "run"])
    with open(PID_FILE, "w") as f:
        f.write(str(proc.pid))
    print(f"gold_data_project đã khởi động (PID: {proc.pid})")
    logger.info(f"gold_data_project đã khởi động (PID: {proc.pid})")
    print("Log được quản lý bởi Python logger")
    logger.info("Log được quản lý bởi Python logger")
    return 0


def run():
    while True:
        try:
            main()
            break  # Nếu hoàn thành, thoát
        except SystemExit:
            print("Thoát một cách duyên dáng.")
            sys.exit(0)
        except Exception as e:
            print(f"Khởi động lại do lỗi: {e}")
            time.sleep(10)


def stop():
    if not os.path.exists(PID_FILE):
        print("Không tìm thấy file PID. Process có đang chạy không?")
        logger.warning("Không tìm thấy file PID. Process có đang chạy không?")
        return 1

    with open(PID_FILE, "r") as f:
        pid = int(f.read().strip())

    if is_process_running(pid):
        print(f"Đang dừng gold_data_project (PID: {pid})...")
        os.kill(pid, signal.SIGTERM)
        time.sleep(2)
        if is_process_running(pid):
            print("Process vẫn đang chạy, buộc dừng...")
            os.kill(pid, signal.SIGKILL)
        os.remove(PID_FILE)
        print("gold_data_project đã dừng")
    else:
        print("Process không chạy, xóa file PID cũ")
        os.remove(PID_FILE)
    return 0


def restart():
    stop()
    time.sleep(2)
    start()


def monitor():
    print("Bắt đầu chế độ giám sát - sẽ tự động khởi động lại khi crash...")
    logger.info("Bắt đầu chế độ giám sát - sẽ tự động khởi động lại khi crash...")
    while True:
        if os.path.exists(PID_FILE):
            with open(PID_FILE, "r") as f:
                pid = int(f.read().strip())
            if not is_process_running(pid):
                print(
                    f"{time.ctime()}: Process bị crash hoặc dừng, đang khởi động lại..."
                )
                logger.warning(
                    f"{time.ctime()}: Process bị crash hoặc dừng, đang khởi động lại..."
                )
                os.remove(PID_FILE)
                start()
        else:
            print(f"{time.ctime()}: Không tìm thấy file PID, đang khởi động process...")
            logger.info(
                f"{time.ctime()}: Không tìm thấy file PID, đang khởi động process..."
            )
            start()
        time.sleep(10)


def status():
    if os.path.exists(PID_FILE):
        with open(PID_FILE, "r") as f:
            pid = int(f.read().strip())
        if is_process_running(pid):
            print(f"gold_data_project đang chạy (PID: {pid})")
        else:
            print("File PID tồn tại nhưng process không chạy")
    else:
        print("gold_data_project không chạy")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        run()
    else:
        parser = argparse.ArgumentParser(
            description="Quản lý dịch vụ gold_data_project"
        )
        parser.add_argument(
            "action",
            choices=["start", "stop", "restart", "monitor", "status"],
            help="Hành động để thực hiện",
        )

        args = parser.parse_args()

        if args.action == "start":
            sys.exit(start())
        elif args.action == "stop":
            sys.exit(stop())
        elif args.action == "restart":
            sys.exit(restart())
        elif args.action == "monitor":
            monitor()
        elif args.action == "status":
            status()
