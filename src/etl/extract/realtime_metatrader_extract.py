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
        self.exchange = exchange or os.getenv(
            "TV_EXCHANGE", "OANDA"
        )  # OANDA có volume data
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
            # Lấy từ phút tiếp theo sau start_time, nhưng kiểm tra không lấy từ tương lai
            now = datetime.now()
            fetch_from = start_time + timedelta(minutes=1)

            # Nếu fetch_from > hiện tại thì không có data mới
            if fetch_from > now:
                self.logger.info(
                    f"No new data: latest DB time is {start_time}, current time {now}"
                )
                return pd.DataFrame()

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

    def get_missing_minute_candles(self):
        """Lấy các nến phút còn thiếu từ lịch sử tới hiện tại"""
        from datetime import datetime, timedelta

        self.logger.info("Checking for missing minute candles...")
        latest_minute = self.get_latest_minute()

        if latest_minute is None:
            self.logger.warning(
                "No historical data found, cannot determine missing candles"
            )
            return pd.DataFrame()

        now = datetime.now()
        current_minute = now.replace(second=0, microsecond=0)

        # Tính số phút thiếu từ latest_minute + 1 phút tới current_minute (không bao gồm current_minute)
        next_minute = latest_minute + timedelta(minutes=1)

        if next_minute >= current_minute:
            self.logger.info(
                f"No missing candles: latest={latest_minute}, current={current_minute}"
            )
            return pd.DataFrame()

        self.logger.info(
            f"Fetching missing candles from {next_minute} to {current_minute - timedelta(minutes=1)}"
        )

        # Lấy data từ next_minute và filter để chỉ lấy các nến đã hoàn thành
        df = self.fetch_realtime_data(latest_minute)

        if df.empty:
            return pd.DataFrame()

        # Lọc chỉ lấy các nến từ next_minute tới trước current_minute (đã hoàn thành)
        missing_candles = df[
            (df["datetime"] >= next_minute) & (df["datetime"] < current_minute)
        ]

        self.logger.info(f"Found {len(missing_candles)} missing completed candles")
        return missing_candles

    def is_data_up_to_date(self):
        """Kiểm tra xem data đã cập nhật tới hiện tại chưa"""
        from datetime import datetime, timedelta

        latest_minute = self.get_latest_minute()
        if latest_minute is None:
            return False

        now = datetime.now()
        current_minute = now.replace(second=0, microsecond=0)

        # Data được coi là up-to-date nếu latest_minute >= current_minute - 1 phút
        # (vì nến hiện tại chưa hoàn thành)
        return latest_minute >= (current_minute - timedelta(minutes=1))

    def get_previous_minute_final_candle(self):
        """Lấy nến cuối cùng của phút trước đó để cập nhật trạng thái cuối cùng"""
        from datetime import datetime, timedelta

        # Lấy thời gian hiện tại và phút trước
        now = datetime.now()
        current_minute = now.replace(second=0, microsecond=0)
        previous_minute = current_minute - timedelta(minutes=1)

        self.logger.info(
            f"Fetching final state of previous minute candle for {previous_minute}"
        )

        # Lấy data gần nhất để đảm bảo có nến phút trước
        fetch_from = previous_minute - timedelta(minutes=1)
        df = self.fetch_realtime_data(fetch_from)

        if df.empty:
            self.logger.warning("No data available for previous minute check")
            return pd.DataFrame()

        # Lọc lấy nến phút trước
        previous_candle = df[df["datetime"] == previous_minute]

        if not previous_candle.empty:
            self.logger.info(
                f"Found previous minute final candle: {previous_minute}, close={previous_candle.iloc[0]['close']}, volume={previous_candle.iloc[0]['volume']}"
            )
            return previous_candle
        else:
            self.logger.warning(
                f"No candle found for previous minute {previous_minute}"
            )
            return pd.DataFrame()

    def realtime_extract(self):
        """Extract dữ liệu realtime: ưu tiên lấy các nến thiếu trước"""
        self.logger.info("Extracting realtime metatrader data ...")

        # Trước tiên lấy các nến phút còn thiếu (đã hoàn thành)
        missing_candles = self.get_missing_minute_candles()

        if not missing_candles.empty:
            self.logger.info(
                f"Extracted {len(missing_candles)} missing completed candles"
            )
            return missing_candles
        else:
            self.logger.info("No missing candles found - data is up to date")
            return pd.DataFrame()
