import pandas as pd
from typing import Optional
from tvDatafeed import TvDatafeed, Interval
from config.logger_config import LoggerConfig

logger = LoggerConfig.logger_config(__name__)


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
        # Khởi tạo TvDatafeed trực tiếp, không qua adapter
        self.tv = TvDatafeed(tv_username or "", tv_password or "")

    def _transform_tv_data(self, df):
        """Transform data từ TradingView về format chuẩn"""
        if df is None or df.empty:
            return pd.DataFrame(
                columns=["datetime", "open", "high", "low", "close", "volume"]
            )

        # Đổi tên cột về chuẩn
        df = df.reset_index()
        df.rename(
            columns={
                "datetime": "date_time",
                "volume": "vol",
            },
            inplace=True,
        )

        # Tách date và time
        df["date"] = df["date_time"].dt.strftime("%Y.%m.%d")
        df["time"] = df["date_time"].dt.strftime("%H:%M:%S")

        # Tạo trường datetime chuẩn
        df["datetime"] = pd.to_datetime(
            df["date"] + " " + df["time"], format="%Y.%m.%d %H:%M:%S"
        )

        # Đổi vol thành volume
        df.rename(columns={"vol": "volume"}, inplace=True)

        # Chỉ giữ các cột cần thiết
        df = df[["datetime", "open", "high", "low", "close", "volume"]]

        # Sắp xếp theo datetime
        df = df.sort_values("datetime").reset_index(drop=True)

        return df

    def get_recent_candles(self, n_candles=10):
        """
        Lấy n nến gần nhất từ TradingView
        Nếu lỗi hoặc không có data, return DataFrame rỗng và pipeline sẽ tự động thử lại ở lần tiếp theo
        """
        try:
            # Gọi trực tiếp TvDatafeed
            df = self.tv.get_hist(
                symbol=self.symbol,
                exchange=self.exchange,
                interval=Interval.in_1_minute,
                n_bars=n_candles,
            )

            # Transform về format chuẩn
            df_transformed = self._transform_tv_data(df)

            if not df_transformed.empty:
                logger.info(f"Đã lấy {len(df_transformed)} nến gần nhất")
            else:
                logger.debug(f"Không có dữ liệu cho {n_candles} nến")

            return df_transformed

        except Exception as e:
            # Bất kỳ lỗi gì cũng chỉ log và return rỗng - pipeline sẽ tự động thử lại
            logger.debug(f"Lỗi lấy dữ liệu (bỏ qua, sẽ thử lại lần sau): {e}")
            return pd.DataFrame(
                columns=["datetime", "open", "high", "low", "close", "volume"]
            )

    def fill_historical_data(self, n_candles=5000):
        """
        Lấy n nến lịch sử để fill data cũ
        Nếu lỗi hoặc không có data, return DataFrame rỗng
        """
        logger.info(f"Đang lấy {n_candles} nến lịch sử")

        try:
            # Gọi trực tiếp TvDatafeed
            df = self.tv.get_hist(
                symbol=self.symbol,
                exchange=self.exchange,
                interval=Interval.in_1_minute,
                n_bars=n_candles,
            )

            # Transform về format chuẩn
            df_transformed = self._transform_tv_data(df)

            if not df_transformed.empty:
                logger.info(f"Đã lấy {len(df_transformed)} nến lịch sử")
            else:
                logger.warning(f"Không có dữ liệu lịch sử cho {n_candles} nến")

            return df_transformed

        except Exception as e:
            # Lỗi historical thì log warning (quan trọng hơn realtime)
            logger.warning(f"Lỗi lấy dữ liệu lịch sử (bỏ qua): {e}")
            return pd.DataFrame(
                columns=["datetime", "open", "high", "low", "close", "volume"]
            )
