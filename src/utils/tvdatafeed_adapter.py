# Adapter để lấy dữ liệu realtime từ TradingView qua tvdatafeed
from typing import Optional
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import logging
import time

logger = logging.getLogger(__name__)


class TVDataFeedAdapter:
    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        max_retries: int = None,
        retry_delay: float = None,
    ):
        """
        Khởi tạo TradingView Data Feed Adapter với cơ chế retry

        Tham số:
            username: TradingView username (tùy chọn)
            password: TradingView password (tùy chọn)
            max_retries: Số lần thử lại tối đa khi gặp lỗi (lấy mặc định từ config)
            retry_delay: Thời gian chờ giữa các lần thử (giây, mặc định từ config)
        """
        # Import ở đây để tránh vòng phụ thuộc (circular dependency)
        from config.variable_config import TRADINGVIEW_CONFIG

        # TvDatafeed yêu cầu credential là chuỗi; truyền chuỗi rỗng nếu None để thỏa kiểm tra kiểu
        self.username = username or ""
        self.password = password or ""
        self.tv = TvDatafeed(self.username, self.password)
        # Import ở đây để tránh vòng phụ thuộc (circular dependency)
        from config.variable_config import TRADINGVIEW_CONFIG

        self.max_retries = max_retries or TRADINGVIEW_CONFIG["max_retries"]
        self.retry_delay = retry_delay or TRADINGVIEW_CONFIG["retry_delay"]

    def get_realtime_data(
        self, symbol, exchange, interval=Interval.in_1_minute, n_bars=None
    ):
        """
        Lấy dữ liệu realtime từ TradingView với cơ chế thử lại (retry)

        Tham số:
            symbol: Tên symbol (ví dụ: XAUUSD)
            exchange: Tên exchange (ví dụ: OANDA)
            interval: Khoảng thời gian (interval)
            n_bars: Số bars cần lấy (mặc định từ config)

        Trả về:
            DataFrame hoặc None nếu thất bại sau tất cả lần thử
        """
        # Import ở đây để tránh vòng phụ thuộc (circular dependency)
        from config.variable_config import TRADINGVIEW_CONFIG

        if n_bars is None:
            n_bars = TRADINGVIEW_CONFIG["default_n_bars"]

        last_exception = None

        for attempt in range(self.max_retries):
            try:
                df = self.tv.get_hist(
                    symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars
                )
                if df is None or df.empty:
                    raise ValueError("Không có dữ liệu trả về từ TradingView")

                # Đổi tên cột về chuẩn
                df = df.reset_index()
                # Columns từ tvDatafeed: ['datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']
                df.rename(
                    columns={
                        "datetime": "date_time",
                        "volume": "vol",  # Giữ volume từ TV thành vol
                    },
                    inplace=True,
                )
                # Tách date và time
                df["date"] = df["date_time"].dt.strftime("%Y.%m.%d")
                df["time"] = df["date_time"].dt.strftime("%H:%M:%S")
                # Chỉ giữ các trường cần thiết (bỏ tickvol và spread)
                df = df[
                    [
                        "date",
                        "time",
                        "open",
                        "high",
                        "low",
                        "close",
                        "vol",
                    ]
                ]

                # Thành công - log nếu đã retry
                if attempt > 0:
                    logger.info(
                        f"Đã lấy dữ liệu thành công cho {symbol}@{exchange} sau {attempt + 1} lần thử"
                    )

                return df

            except (TimeoutError, ConnectionError, OSError) as e:
                # Các lỗi network có thể retry
                last_exception = e
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (
                        2**attempt
                    )  # Tăng thời gian chờ theo hàm mũ (exponential backoff)
                    logger.warning(
                        f"Lỗi mạng khi lấy {symbol}@{exchange} "
                        f"(lần thử {attempt + 1}/{self.max_retries}): {e}. "
                        f"Sẽ thử lại sau {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    # Tạo lại kết nối sau mỗi lỗi mạng
                    try:
                        self.tv = TvDatafeed(self.username, self.password)
                    except Exception:
                        pass
                else:
                    logger.error(
                        f"Không thể lấy dữ liệu cho {symbol}@{exchange} sau {self.max_retries} lần thử: {e}"
                    )

            except ValueError as e:
                # Lỗi "No data returned" - có thể do symbol/exchange sai
                last_exception = e
                logger.error(f"Lỗi ValueError cho {symbol}@{exchange}: {e}")
                # Không retry cho ValueError
                break

            except Exception as e:
                # Các lỗi khác - log và break
                last_exception = e
                logger.exception(
                    f"Lỗi không mong muốn khi lấy dữ liệu {symbol}@{exchange}: {e}"
                )
                break

        # Tất cả retries đều thất bại
        logger.error(
            f"Tất cả {self.max_retries} lần thử đều thất bại cho {symbol}@{exchange}. "
            f"Lỗi cuối cùng: {last_exception}"
        )
        return None
