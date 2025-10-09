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

        # TradingView adapter
        self.symbol = symbol or os.getenv("TV_SYMBOL", "XAUUSD")
        self.exchange = exchange or os.getenv("TV_EXCHANGE", "FOREXCOM")
        self.tv_adapter = TVDataFeedAdapter(tv_username, tv_password)

    def get_latest_minute(self):
        latest = self.gold_collection.find_one({}, sort=[("date", -1), ("time", -1)])
        if latest:
            dt_str = f"{latest['date']} {latest['time']}"
            dt = datetime.strptime(dt_str, "%Y.%m.%d %H:%M:%S")
            # Làm tròn lên phút
            dt = dt.replace(second=0)
            return dt
        else:
            return None

    def fetch_realtime_data(self, start_time: datetime | None = None) -> pd.DataFrame:
        # Use TVDataFeedAdapter to fetch minute bars since start_time (exclusive)
        # If start_time is None, fetch the last 5000 bars (adapter default)
        if start_time:
            # fetch from one minute after the latest stored
            fetch_from = start_time + timedelta(minutes=1)
            # tvdatafeed returns up to n_bars back from now; to get since fetch_from we fetch recent bars and then filter
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
                    "date",
                    "time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "tickvol",
                    "vol",
                    "spread",
                ]
            )

        # parse datetime to filter
        df["date_time"] = pd.to_datetime(
            df["date"] + " " + df["time"], format="%Y.%m.%d %H:%M:%S"
        )
        if fetch_from:
            df = df[df["date_time"] >= fetch_from]

        # drop the helper column
        df.drop(columns=["date_time"], inplace=True)

        # ensure types
        df = df.drop_duplicates(subset=["date", "time"]).reset_index(drop=True)
        return df

    def realtime_extract(self):
        self.logger.info("Extracting realtime metatrader data ...")
        latest_minute = self.get_latest_minute()
        df = self.fetch_realtime_data(latest_minute)
        self.logger.info(f"Extracted {len(df)} records from {latest_minute} to now.")
        return df
