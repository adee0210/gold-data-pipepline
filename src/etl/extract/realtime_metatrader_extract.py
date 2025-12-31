import pandas as pd
from typing import Optional
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG
from src.utils.tvdatafeed_adapter import TVDataFeedAdapter
import uuid


class RealtimeMetatraderExtract:
    def __init__(
        self,
        tv_username: Optional[str] = None,
        tv_password: Optional[str] = None,
        symbol: Optional[str] = "XAUUSD",
        exchange: Optional[str] = "OANDA",
    ):
        self.symbol = symbol
        self.exchange = exchange
        self.tv_adapter = TVDataFeedAdapter(tv_username, tv_password)

    def get_recent_candles(self, n_candles=10):
        """Lấy n nến gần nhất từ TradingView"""
        print(f"Đang lấy {n_candles} nến gần nhất - Unique ID: {uuid.uuid4()}")
        df = self.tv_adapter.get_realtime_data(
            symbol=self.symbol, exchange=self.exchange, n_bars=n_candles
        )
        if df is None or df.empty:
            print("Không có dữ liệu trả về từ TV adapter")
            return pd.DataFrame(
                columns=[
                    "datetime",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                ]
            )

        # Tạo trường datetime
        df["datetime"] = pd.to_datetime(
            df["date"] + " " + df["time"], format="%Y.%m.%d %H:%M:%S"
        )

        # Sắp xếp theo datetime
        df = df.sort_values("datetime").reset_index(drop=True)

        # Rename vol to volume if it exists
        if "vol" in df.columns:
            df.rename(columns={"vol": "volume"}, inplace=True)
        elif "volume" not in df.columns:
            df["volume"] = None  # Ensure volume column exists with default None

        print(f"Đã lấy {len(df)} nến gần nhất")
        return df

    def fill_historical_data(self, n_candles=5000):
        """Lấy n nến lịch sử để fill data cũ"""
        print(f"Đang lấy {n_candles} nến lịch sử")
        df = self.tv_adapter.get_realtime_data(
            symbol=self.symbol, exchange=self.exchange, n_bars=n_candles
        )
        if df is None or df.empty:
            print("Không có dữ liệu lịch sử trả về từ TV adapter")
            return pd.DataFrame(
                columns=[
                    "datetime",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                ]
            )

        # Tạo trường datetime
        df["datetime"] = pd.to_datetime(
            df["date"] + " " + df["time"], format="%Y.%m.%d %H:%M:%S"
        )

        # Sắp xếp theo datetime
        df = df.sort_values("datetime").reset_index(drop=True)

        # Rename vol to volume if it exists
        if "vol" in df.columns:
            df.rename(columns={"vol": "volume"}, inplace=True)
        elif "volume" not in df.columns:
            df["volume"] = None  # Ensure volume column exists with default None

        print(f"Đã lấy {len(df)} nến lịch sử")
        return df
