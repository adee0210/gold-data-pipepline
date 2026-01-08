# Adapter để lấy dữ liệu realtime từ TradingView qua tvdatafeed
from typing import Optional
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import logging
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.logger_config import LoggerConfig

logger = LoggerConfig.logger_config(__name__)


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
        Lấy dữ liệu realtime từ TradingView - KHÔNG RETRY, chỉ thử 1 lần
        Pipeline đã chạy liên tục, nếu lỗi thì bỏ qua và đợi lần fetch tiếp theo

        Tham số:
            symbol: Tên symbol (ví dụ: XAUUSD)
            exchange: Tên exchange (ví dụ: OANDA)
            interval: Khoảng thời gian (interval)
            n_bars: Số bars cần lấy (mặc định từ config)

        Trả về:
            DataFrame hoặc None nếu thất bại
        """
        from config.variable_config import TRADINGVIEW_CONFIG

        if n_bars is None:
            n_bars = TRADINGVIEW_CONFIG["default_n_bars"]

        try:
            df = self.tv.get_hist(
                symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars
            )
            if df is None or df.empty:
                # Không log error nữa vì có thể thị trường đóng cửa - chỉ warning
                logger.debug(f"Không có dữ liệu từ TradingView cho {symbol}@{exchange}")
                return None

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
            # Chỉ giữ các trường cần thiết
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

            return df

        except ValueError as e:
            # Không có dữ liệu - có thể thị trường đóng cửa, chỉ log debug
            logger.debug(f"ValueError cho {symbol}@{exchange}: {e}")
            return None

        except (TimeoutError, ConnectionError, OSError) as e:
            # Lỗi network - log warning, không retry
            logger.warning(f"Lỗi mạng khi lấy {symbol}@{exchange}: {e}")
            # Tạo lại kết nối cho lần sau
            try:
                self.tv = TvDatafeed(self.username, self.password)
            except Exception:
                pass
            return None

        except Exception as e:
            # Lỗi khác - chỉ log 1 lần
            logger.error(f"Lỗi khi lấy dữ liệu {symbol}@{exchange}: {e}")
            return None
