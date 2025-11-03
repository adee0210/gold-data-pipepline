import requests
import time
from datetime import datetime, timedelta
from typing import Optional
from config.variable_config import DISCORD_CONFIG
from config.logger_config import LoggerConfig


class DiscordAlertUtil:
    """
    Class để gửi cảnh báo lỗi về Discord khi có vấn đề với data extraction.
    Chỉ gửi cảnh báo khi có lỗi, không gửi khi thành công.
    Tự động bỏ qua cảnh báo vào T7/CN khi thị trường đóng cửa.
    """

    def __init__(self):
        self.logger = LoggerConfig.logger_config("Discord Alert")
        self.webhook_url = DISCORD_CONFIG["webhook_url"]
        self.enabled = DISCORD_CONFIG["enabled"]

        self.last_alert_times = {}
        self.alert_cooldown = timedelta(minutes=5)  # Chỉ gửi lại sau 5 phút

        self.last_successful_data_time = {}
        self.no_data_threshold = timedelta(
            minutes=1
        )  # Cảnh báo sau 1 phút không có data

        if not self.enabled:
            self.logger.info("Discord alerts are disabled")
        elif not self.webhook_url:
            self.logger.warning("Discord webhook URL not configured")
            self.enabled = False
        else:
            self.logger.info("Discord alerts are enabled")

    def _is_weekend(self, dt: Optional[datetime] = None) -> bool:
        """
        Kiểm tra xem có phải cuối tuần không (Thứ 7 hoặc Chủ nhật)

        Args:
            dt: Datetime để kiểm tra (None = hiện tại)

        Returns:
            bool: True nếu là T7 (5) hoặc CN (6)
        """
        if dt is None:
            dt = datetime.now()
        return dt.weekday() in [5, 6]  # 5=Saturday, 6=Sunday

    def _is_market_closed_time(self, dt: Optional[datetime] = None) -> bool:
        """
        Kiểm tra xem có phải thời gian thị trường đóng cửa không
        Thị trường vàng thường đóng cửa:
        - Toàn bộ Chủ nhật (weekday=6)
        - Thứ 7 sau khoảng 12h trưa (weekday=5 và sau 12:00)

        Args:
            dt: Datetime để kiểm tra (None = hiện tại)

        Returns:
            bool: True nếu thị trường đóng cửa
        """
        if dt is None:
            dt = datetime.now()

        weekday = dt.weekday()

        # Chủ nhật: Thị trường đóng cửa cả ngày
        if weekday == 6:
            return True

        # Thứ 7: Thị trường có thể đóng cửa sau 12h trưa
        # (Tùy múi giờ, có thể điều chỉnh giờ này)
        if weekday == 5:
            # Coi như sau 6h sáng thứ 7 là đã đóng cửa
            # (vì thị trường mở muộn hơn trong tuần)
            if dt.hour >= 6:
                return True

        return False

    def _should_send_alert(self, alert_key: str) -> bool:
        """
        Kiểm tra xem có nên gửi cảnh báo không (để tránh spam)

        Args:
            alert_key: Key để tracking thời gian cảnh báo cuối

        Returns:
            bool: True nếu nên gửi cảnh báo
        """
        if not self.enabled:
            return False

        now = datetime.now()
        last_alert = self.last_alert_times.get(alert_key)

        if last_alert is None:
            return True

        # Chỉ gửi lại nếu đã qua cooldown period
        if now - last_alert >= self.alert_cooldown:
            return True

        return False

    def _send_discord_message(self, message: str, alert_key: str) -> bool:
        """
        Gửi message tới Discord webhook

        Args:
            message: Nội dung cảnh báo
            alert_key: Key để tracking

        Returns:
            bool: True nếu gửi thành công
        """
        if not self.enabled:
            return False

        if not self._should_send_alert(alert_key):
            self.logger.debug(f"Skipping alert {alert_key} - cooldown period")
            return False

        try:
            payload = {"content": message}

            response = requests.post(self.webhook_url, json=payload, timeout=10)

            if response.status_code == 204:
                self.logger.info(f"Đã gửi cảnh báo Discord thành công: {alert_key}")
                self.last_alert_times[alert_key] = datetime.now()
                return True
            else:
                self.logger.error(
                    f"Lỗi gửi Discord alert. Status code: {response.status_code}, Response: {response.text}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Exception khi gửi Discord alert: {str(e)}")
            return False

    def alert_no_data_from_source(
        self, source: str, error_details: Optional[str] = None
    ):
        """
        Cảnh báo khi không lấy được data từ nguồn
        Tự động bỏ qua nếu là T7/CN (thị trường đóng cửa)

        Args:
            source: Tên nguồn data (VD: TradingView, MetaTrader)
            error_details: Chi tiết lỗi nếu có
        """
        # Kiểm tra nếu là cuối tuần - thị trường đóng cửa
        if self._is_market_closed_time():
            self.logger.info(
                f"Bỏ qua cảnh báo no_data từ {source} - Thị trường đóng cửa (T7/CN)"
            )
            return

        alert_key = f"no_data_{source}"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"**CẢNH BÁO: Không có dữ liệu từ {source}**\n"
        message += f"Thời gian: {timestamp}\n"

        if error_details:
            message += f"Chi tiết: {error_details}\n"

        message += f"Hệ thống không lấy được dữ liệu mới từ nguồn {source}"

        self._send_discord_message(message, alert_key)

    def alert_data_fetch_error(self, source: str, error_message: str):
        """
        Cảnh báo khi có lỗi khi lấy data

        Args:
            source: Tên nguồn data
            error_message: Thông điệp lỗi
        """
        alert_key = f"fetch_error_{source}"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"**LỖI: Không thể lấy dữ liệu từ {source}**\n"
        message += f"Thời gian: {timestamp}\n"
        message += f"Lỗi: {error_message}\n"
        message += f"Vui lòng kiểm tra kết nối và cấu hình"

        self._send_discord_message(message, alert_key)

    def alert_data_format_error(self, source: str, error_details: str):
        """
        Cảnh báo khi có lỗi định dạng data

        Args:
            source: Tên nguồn data
            error_details: Chi tiết lỗi định dạng
        """
        alert_key = f"format_error_{source}"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"**LỖI ĐỊNH DẠNG DỮ LIỆU từ {source}**\n"
        message += f"Thời gian: {timestamp}\n"
        message += f"Chi tiết: {error_details}\n"
        message += f"Dữ liệu nhận được không đúng định dạng mong đợi"

        self._send_discord_message(message, alert_key)

    def alert_no_new_data(self, source: str, last_data_time: Optional[datetime] = None):
        """
        Cảnh báo khi không có data mới sau 1 phút
        Tự động bỏ qua nếu là T7/CN (thị trường đóng cửa)

        Args:
            source: Tên nguồn data
            last_data_time: Thời gian của data cuối cùng
        """
        # Kiểm tra nếu là cuối tuần - thị trường đóng cửa
        if self._is_market_closed_time():
            self.logger.info(
                f"Bỏ qua cảnh báo no_new_data từ {source} - Thị trường đóng cửa (T7/CN)"
            )
            return

        alert_key = f"no_new_data_{source}"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"**CẢNH BÁO: Không có dữ liệu mới từ {source}**\n"
        message += f"Thời gian: {timestamp}\n"

        if last_data_time:
            message += f"Dữ liệu cuối: {last_data_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            time_diff = datetime.now() - last_data_time
            minutes = int(time_diff.total_seconds() / 60)
            message += f"Đã {minutes} phút không có dữ liệu mới\n"

        message += f"Hệ thống không nhận được dữ liệu mới trong 1 phút qua"

        self._send_discord_message(message, alert_key)

    def alert_database_error(self, operation: str, error_message: str):
        """
        Cảnh báo khi có lỗi database

        Args:
            operation: Thao tác đang thực hiện (insert, update, query...)
            error_message: Thông điệp lỗi
        """
        alert_key = f"db_error_{operation}"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"**LỖI DATABASE: {operation}**\n"
        message += f"Thời gian: {timestamp}\n"
        message += f"Lỗi: {error_message}\n"
        message += f"Vui lòng kiểm tra kết nối database"

        self._send_discord_message(message, alert_key)

    def alert_gap_detected(
        self, start_time: datetime, end_time: datetime, gap_minutes: int
    ):
        """
        Cảnh báo khi phát hiện khoảng trống trong dữ liệu
        Tự động bỏ qua nếu khoảng trống bắt đầu trong T7/CN (thị trường đóng cửa)

        Args:
            start_time: Thời điểm bắt đầu khoảng trống
            end_time: Thời điểm kết thúc khoảng trống
            gap_minutes: Số phút bị thiếu
        """
        # Kiểm tra nếu gap BẮT ĐẦU trong thời gian đóng cửa (T7/CN)
        # Gap từ cuối tuần sang tuần mới là BÌNH THƯỜNG, không cần alert
        if self._is_market_closed_time(start_time):
            self.logger.info(
                f"Bỏ qua cảnh báo gap_detected [{start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')}] "
                f"({gap_minutes} phút) - Gap bắt đầu trong thời gian thị trường đóng cửa (T7/CN)"
            )
            return

        alert_key = f"gap_detected_{start_time.strftime('%Y%m%d%H%M')}"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"**PHÁT HIỆN KHOẢNG TRỐNG DỮ LIỆU**\n"
        message += f"Phát hiện lúc: {timestamp}\n"
        message += f"Khoảng trống từ: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        message += f"Đến: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        message += f"Thiếu: {gap_minutes} phút dữ liệu\n"
        message += f"Hệ thống sẽ cố gắng lấy dữ liệu thiếu"

        self._send_discord_message(message, alert_key)

    def check_and_alert_no_new_data(
        self, source: str, current_data_time: Optional[datetime] = None
    ):
        """
        Kiểm tra và cảnh báo nếu không có data mới sau 1 phút
        Tự động bỏ qua nếu là T7/CN (thị trường đóng cửa)

        Args:
            source: Tên nguồn data
            current_data_time: Thời gian của data hiện tại (None nếu không có data)
        """
        # Kiểm tra nếu là cuối tuần - thị trường đóng cửa
        if self._is_market_closed_time():
            # Vẫn cập nhật tracking time nhưng không cảnh báo
            tracking_key = f"data_time_{source}"
            self.last_successful_data_time[tracking_key] = datetime.now()
            return
        now = datetime.now()
        tracking_key = f"data_time_{source}"

        # Nếu có data mới, cập nhật thời gian
        if current_data_time:
            self.last_successful_data_time[tracking_key] = now
            return

        # Kiểm tra xem đã bao lâu không có data mới
        last_success = self.last_successful_data_time.get(tracking_key)

        if last_success:
            time_since_last_data = now - last_success

            # Nếu quá 1 phút không có data mới, gửi cảnh báo
            if time_since_last_data >= self.no_data_threshold:
                self.alert_no_new_data(source, last_success)
        else:
            # Lần đầu tiên check, không gửi cảnh báo ngay
            self.last_successful_data_time[tracking_key] = now
