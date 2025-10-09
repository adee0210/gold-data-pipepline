# Adapter để lấy dữ liệu realtime từ TradingView qua tvdatafeed
from typing import Optional
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class TVDataFeedAdapter:
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        # TvDatafeed expects string credentials; pass empty string when None to satisfy type checks
        self.tv = TvDatafeed(username or "", password or "")

    def get_realtime_data(
        self, symbol, exchange, interval=Interval.in_1_minute, n_bars=5000
    ):
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
            return df
        except Exception as e:
            logger.exception(f"Error fetching data for {symbol}@{exchange}: {e}")
            return None
