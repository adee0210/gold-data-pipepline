import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG
from src.utils.tvdatafeed_adapter import TVDataFeedAdapter
import os


class RealtimeMetatraderExtract:
    def __init__(
        self,
        tv_username: Optional[str] = None,
        tv_password: Optional[str] = None,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
    ):
        self.logger = LoggerConfig.logger_config(
            "Extract Realtime Metatrader gold data"
        )
        self.mongo_config = MongoConfig()
        self.mongo_client = self.mongo_config.get_client()
        self.gold_db = self.mongo_client.get_database(GOLD_DATA_CONFIG["database"])
        self.gold_collection = self.gold_db.get_collection(
            GOLD_DATA_CONFIG["collection"]
        )

        self.symbol = symbol or os.getenv("TV_SYMBOL", "XAUUSD")
        self.exchange = exchange or os.getenv("TV_EXCHANGE", "FOREXCOM")
        self.tv_adapter = TVDataFeedAdapter(tv_username, tv_password)

    def get_latest_minute(self):
        latest = self.gold_collection.find_one({}, sort=[("datetime", -1)])
        if latest:
            dt = latest["datetime"]
            dt = dt.replace(second=0, microsecond=0)
            return dt
        else:
            return None

    def fetch_realtime_data(self, start_time: datetime | None = None) -> pd.DataFrame:
        if start_time:
            fetch_from = start_time + timedelta(minutes=1)
            self.logger.info(
                f"Fetching TradingView data for {self.symbol}@{self.exchange} since {fetch_from}"
            )
        else:
            fetch_from = None
            self.logger.info(
                f"Fetching recent TradingView data for {self.symbol}@{self.exchange}"
            )

        df = self.tv_adapter.get_realtime_data(
            symbol=self.symbol, exchange=self.exchange
        )
        if df is None or df.empty:
            self.logger.warning("No data returned from TV adapter")
            return pd.DataFrame(
                columns=[
                    "datetime",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",  # Đổi từ vol thành volume để match historical schema
                ]
            )

        # Create datetime field by combining date and time
        df["datetime"] = pd.to_datetime(
            df["date"] + " " + df["time"], format="%Y.%m.%d %H:%M:%S"
        )
        if fetch_from:
            df = df[df["datetime"] >= fetch_from]

        # Đổi tên vol thành volume để match historical schema
        df = df.rename(columns={"vol": "volume"})
        # Drop the separate date and time columns, keep datetime
        df.drop(columns=["date", "time"], inplace=True)

        df = df.drop_duplicates(subset=["datetime"]).reset_index(drop=True)
        return df

    def get_current_minute_candle(self):
        """Lấy nến phút hiện tại để upsert liên tục"""
        from datetime import datetime, timedelta

        # Lấy thời gian hiện tại và làm tròn về phút
        now = datetime.now()
        current_minute = now.replace(second=0, microsecond=0)

        self.logger.info(f"Fetching current minute candle for {current_minute}")

        # Lấy data từ 2 phút gần nhất để đảm bảo có dữ liệu phút hiện tại
        fetch_from = current_minute - timedelta(minutes=2)
        df = self.fetch_realtime_data(fetch_from)

        if df.empty:
            return pd.DataFrame()

        # Lọc chỉ lấy nến phút hiện tại
        current_candle = df[df["datetime"] == current_minute]

        if not current_candle.empty:
            self.logger.info(
                f"Found current minute candle: close={current_candle.iloc[0]['close']}, volume={current_candle.iloc[0]['volume']}"
            )
            return current_candle
        else:
            self.logger.warning(f"No candle found for current minute {current_minute}")
            return pd.DataFrame()

    def realtime_extract(self):
        self.logger.info("Extracting realtime metatrader data ...")
        latest_minute = self.get_latest_minute()
        df = self.fetch_realtime_data(latest_minute)
        self.logger.info(f"Extracted {len(df)} records from {latest_minute} to now.")
        return df
