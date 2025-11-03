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
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        """
        Initialize TradingView Data Feed Adapter với retry logic

        Args:
            username: TradingView username (optional)
            password: TradingView password (optional)
            max_retries: Số lần retry tối đa khi gặp lỗi (default: 3)
            retry_delay: Thời gian chờ giữa các lần retry (seconds, default: 2.0)
        """
        # TvDatafeed expects string credentials; pass empty string when None to satisfy type checks
        self.username = username or ""
        self.password = password or ""
        self.tv = TvDatafeed(self.username, self.password)
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def get_realtime_data(
        self, symbol, exchange, interval=Interval.in_1_minute, n_bars=5000
    ):
        """
        Lấy dữ liệu realtime từ TradingView với retry logic

        Args:
            symbol: Symbol name (e.g., XAUUSD)
            exchange: Exchange name (e.g., OANDA)
            interval: Time interval
            n_bars: Number of bars to fetch

        Returns:
            DataFrame hoặc None nếu thất bại sau tất cả retries
        """
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                df = self.tv.get_hist(
                    symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars
                )
                if df is None or df.empty:
                    raise ValueError("No data returned from TradingView")

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
                        f"Successfully fetched data for {symbol}@{exchange} "
                        f"after {attempt + 1} attempt(s)"
                    )

                return df

            except (TimeoutError, ConnectionError, OSError) as e:
                # Các lỗi network có thể retry
                last_exception = e
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        f"Network error fetching {symbol}@{exchange} "
                        f"(attempt {attempt + 1}/{self.max_retries}): {e}. "
                        f"Retrying in {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    # Recreate connection sau mỗi network error
                    try:
                        self.tv = TvDatafeed(self.username, self.password)
                    except Exception:
                        pass
                else:
                    logger.error(
                        f"Failed to fetch {symbol}@{exchange} after {self.max_retries} attempts: {e}"
                    )

            except ValueError as e:
                # Lỗi "No data returned" - có thể do symbol/exchange sai
                last_exception = e
                logger.error(f"ValueError for {symbol}@{exchange}: {e}")
                # Không retry cho ValueError
                break

            except Exception as e:
                # Các lỗi khác - log và break
                last_exception = e
                logger.exception(f"Unexpected error fetching {symbol}@{exchange}: {e}")
                break

        # Tất cả retries đều thất bại
        logger.error(
            f"All {self.max_retries} attempts failed for {symbol}@{exchange}. "
            f"Last error: {last_exception}"
        )
        return None
