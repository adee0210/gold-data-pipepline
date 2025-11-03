import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Optional
from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG
from src.utils.tvdatafeed_adapter import TVDataFeedAdapter
from src.utils.discord_alert_util import DiscordAlertUtil
from tvDatafeed import TvDatafeed, Interval
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

        # Khởi tạo Discord alert utility
        self.discord_alert = DiscordAlertUtil()

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
            # KHÔNG gửi alert ngay lập tức - để logic check_and_alert_no_new_data xử lý
            # Alert chỉ được gửi sau 1 phút không có data mới
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
            # Cập nhật thời gian có data thành công
            self.discord_alert.check_and_alert_no_new_data(
                source="TradingView_Realtime", current_data_time=current_minute
            )
            return current_candle
        else:
            self.logger.warning(f"No candle found for current minute {current_minute}")
            # Kiểm tra và cảnh báo nếu không có data mới
            self.discord_alert.check_and_alert_no_new_data(
                source="TradingView_Realtime", current_data_time=None
            )
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

    def fetch_historical_range(self, start_time, end_time):
        """
        Lấy dữ liệu lịch sử trong khoảng thời gian cụ thể

        Args:
            start_time (datetime): Thời điểm bắt đầu
            end_time (datetime): Thời điểm kết thúc

        Returns:
            DataFrame: DataFrame chứa dữ liệu trong khoảng thời gian
        """
        self.logger.info(f"Lấy dữ liệu lịch sử từ {start_time} đến {end_time}")

        # Tính số phút trong khoảng
        time_range_minutes = int((end_time - start_time).total_seconds() // 60) + 1

        if time_range_minutes > 5000:
            self.logger.warning(
                f"Khoảng thời gian quá lớn: {time_range_minutes} phút > 5000 phút"
            )
            # Chia nhỏ khoảng thời gian thành các phần 5000 phút
            all_data = []
            current_start = start_time

            while current_start <= end_time:
                current_end = min(current_start + timedelta(minutes=4999), end_time)
                chunk_df = self._fetch_chunk(current_start, current_end)

                if chunk_df is not None and not chunk_df.empty:
                    all_data.append(chunk_df)

                # Cập nhật thời gian bắt đầu cho chunk tiếp theo
                current_start = current_end + timedelta(minutes=1)
                # Tạm nghỉ để tránh quá tải API
                time.sleep(2)

            # Ghép các chunk lại với nhau
            if all_data:
                result_df = pd.concat(all_data)
                return result_df
            else:
                return None
        else:
            return self._fetch_chunk(start_time, end_time)

    def _fetch_chunk(self, start_time, end_time):
        """
        Helper method để lấy một chunk dữ liệu
        """
        try:
            # Khởi tạo TvDatafeed
            tv = TvDatafeed()

            # Tính số phút
            time_range_minutes = int((end_time - start_time).total_seconds() // 60) + 1

            # Lấy dữ liệu
            df = tv.get_hist(
                symbol=self.symbol,
                exchange=self.exchange,
                interval=Interval.in_1_minute,
                n_bars=time_range_minutes,
            )

            if df is None or df.empty:
                self.logger.warning(
                    f"Không lấy được dữ liệu từ TradingView cho khoảng {start_time} đến {end_time}"
                )
                # Gửi cảnh báo Discord
                self.discord_alert.alert_no_data_from_source(
                    source="TradingView_Historical",
                    error_details=f"Không có dữ liệu cho khoảng {start_time} đến {end_time}",
                )
                return None

            # Format dữ liệu
            df = df.reset_index()

            # Tạo trường datetime đúng từ index
            df["datetime"] = pd.to_datetime(df["datetime"])

            # Filter dữ liệu trong khoảng
            df = df[(df["datetime"] >= start_time) & (df["datetime"] <= end_time)]

            if df.empty:
                self.logger.warning(
                    f"Sau khi filter, không còn dữ liệu cho khoảng {start_time} đến {end_time}"
                )
                return None

            # Đổi tên volume
            if "volume" in df.columns:
                # Giữ nguyên nếu đã có tên đúng
                pass
            else:
                # Đổi tên nếu cần thiết
                df = df.rename(columns={"vol": "volume"})

            self.logger.info(
                f"Đã lấy được {len(df)} records từ TradingView cho khoảng {start_time} đến {end_time}"
            )
            return df

        except Exception as e:
            self.logger.exception(
                f"Lỗi khi lấy dữ liệu lịch sử cho khoảng {start_time} đến {end_time}: {e}"
            )
            # Gửi cảnh báo Discord khi có exception
            self.discord_alert.alert_data_fetch_error(
                source="TradingView_Historical",
                error_message=f"Exception khi lấy dữ liệu cho khoảng {start_time} đến {end_time}: {str(e)}",
            )
            return None

    def check_and_fix_gaps(self, lookback_hours=24, start_date=None, end_date=None):
        """
        Kiểm tra và sửa dữ liệu thiếu trong khoảng thời gian lookback_hours
        hoặc khoảng thời gian cụ thể từ start_date đến end_date
        Cải tiến: Lọc ra chỉ những dữ liệu còn thiếu thực sự

        Args:
            lookback_hours (int): Số giờ cần kiểm tra ngược về quá khứ (khi không chỉ định start_date và end_date)
            start_date (datetime): Thời gian bắt đầu kiểm tra (nếu chỉ định)
            end_date (datetime): Thời gian kết thúc kiểm tra (nếu chỉ định)

        Returns:
            DataFrame: DataFrame chứa dữ liệu thiếu cần cập nhật
        """
        now = datetime.now()

        # Nếu có chỉ định start_date và end_date thì dùng chúng
        if start_date and end_date:
            start_time = start_date
            end_time = end_date
            self.logger.info(
                f"Bắt đầu kiểm tra dữ liệu thiếu từ {start_time} đến {end_time} (khoảng thời gian cụ thể)"
            )
        else:
            # Ngược lại, dùng lookback_hours
            self.logger.info(
                f"Bắt đầu kiểm tra dữ liệu thiếu trong {lookback_hours} giờ qua"
            )

            # Thời gian bắt đầu kiểm tra
            start_time = now - timedelta(hours=lookback_hours)
            start_time = start_time.replace(second=0, microsecond=0)

            # Thời gian kết thúc kiểm tra: thời điểm hiện tại - 1 phút
            end_time = now.replace(second=0, microsecond=0) - timedelta(minutes=1)

        self.logger.info(f"Kiểm tra dữ liệu từ {start_time} đến {end_time}")

        # Lấy các records trong khoảng thời gian
        records = list(
            self.gold_collection.find(
                {"datetime": {"$gte": start_time, "$lte": end_time}}
            ).sort("datetime", 1)
        )

        if not records:
            self.logger.warning(
                f"Không có dữ liệu nào trong khoảng {start_time} đến {end_time}!"
            )
            # Lấy lại toàn bộ dữ liệu
            self.logger.info(f"Chuẩn bị lấy dữ liệu cho toàn bộ khoảng thời gian")
            df = self.fetch_historical_range(start_time, end_time)

            if df is not None and not df.empty:
                # Trả về DataFrame để load method sẽ xử lý việc lưu vào database
                return df
            else:
                self.logger.error(
                    f"Không lấy được dữ liệu cho toàn bộ khoảng thời gian!"
                )
                return pd.DataFrame()

        # Tìm các khoảng thiếu
        missing_ranges = []
        current_time = start_time

        for record in records:
            record_time = record["datetime"]

            # Nếu có khoảng trống > 1 phút
            if record_time > current_time + timedelta(minutes=1):
                missing_ranges.append(
                    (current_time, record_time - timedelta(minutes=1))
                )

            # Cập nhật current_time
            current_time = record_time + timedelta(minutes=1)

        # Kiểm tra khoảng trống cuối cùng
        if end_time > records[-1]["datetime"] + timedelta(minutes=1):
            missing_ranges.append(
                (records[-1]["datetime"] + timedelta(minutes=1), end_time)
            )

        # Lọc bỏ các khoảng trống nằm trong T7/CN (thị trường đóng cửa)
        filtered_missing_ranges = []
        for start_gap, end_gap in missing_ranges:
            # Kiểm tra nếu khoảng trống nằm hoàn toàn trong thời gian thị trường đóng cửa
            if self.discord_alert._is_market_closed_time(
                start_gap
            ) and self.discord_alert._is_market_closed_time(end_gap):
                gap_minutes = int((end_gap - start_gap).total_seconds() // 60) + 1
                self.logger.info(
                    f"Bỏ qua khoảng trống từ {start_gap} đến {end_gap} ({gap_minutes} phút) "
                    f"- Thị trường đóng cửa (T7/CN)"
                )
                continue
            filtered_missing_ranges.append((start_gap, end_gap))

        missing_ranges = filtered_missing_ranges

        if not missing_ranges:
            self.logger.info(
                f"Không tìm thấy khoảng trống dữ liệu trong {lookback_hours} giờ qua "
                f"(đã loại bỏ T7/CN)"
            )
            return pd.DataFrame()

        # Tính tổng số phút thiếu
        total_missing_minutes = sum(
            (end - start).total_seconds() // 60 + 1 for start, end in missing_ranges
        )
        self.logger.info(
            f"Tìm thấy {len(missing_ranges)} khoảng trống với tổng cộng {int(total_missing_minutes)} phút dữ liệu bị thiếu"
        )

        # Gửi cảnh báo Discord về các khoảng trống lớn (> 5 phút)
        for start_gap, end_gap in missing_ranges:
            gap_minutes = int((end_gap - start_gap).total_seconds() // 60) + 1
            if gap_minutes > 5:  # Chỉ cảnh báo khoảng trống lớn hơn 5 phút
                self.discord_alert.alert_gap_detected(start_gap, end_gap, gap_minutes)

        # Lấy dữ liệu cho từng khoảng trống
        all_gap_data = []
        for start_gap, end_gap in missing_ranges:
            # Log thông tin khoảng trống
            gap_minutes = int((end_gap - start_gap).total_seconds() // 60) + 1
            if gap_minutes > 1:
                self.logger.info(
                    f"Đang lấy dữ liệu cho khoảng trống: {start_gap} đến {end_gap} ({gap_minutes} phút)"
                )
            else:
                self.logger.info(f"Đang lấy dữ liệu cho phút thiếu: {start_gap}")

            # Lấy dữ liệu
            df = self.fetch_historical_range(start_gap, end_gap)

            if df is not None and not df.empty:
                all_gap_data.append(df)
                self.logger.info(f"Đã lấy được {len(df)} records")
            else:
                self.logger.warning(
                    f"Không lấy được dữ liệu cho khoảng trống {start_gap} đến {end_gap}"
                )

        # Ghép tất cả dữ liệu thiếu lại
        if all_gap_data:
            result_df = pd.concat(all_gap_data)
            self.logger.info(
                f"Tổng cộng đã lấy được {len(result_df)} records dữ liệu thiếu"
            )
            return result_df
        else:
            self.logger.warning("Không lấy được dữ liệu thiếu nào!")
            return pd.DataFrame()

    def fetch_latest_n_bars(self, n_bars=5000):
        """
        Lấy n_bars dữ liệu mới nhất từ TradingView

        Args:
            n_bars (int): Số lượng bars cần lấy

        Returns:
            tuple: (DataFrame chứa dữ liệu mới nhất, datetime cũ nhất của dữ liệu)
        """
        self.logger.info(f"Lấy {n_bars} bars dữ liệu mới nhất từ TradingView")

        try:
            # Khởi tạo TvDatafeed
            tv = TvDatafeed()

            # Lấy dữ liệu mới nhất
            df = tv.get_hist(
                symbol=self.symbol,
                exchange=self.exchange,
                interval=Interval.in_1_minute,
                n_bars=n_bars,
            )

            if df is None or df.empty:
                self.logger.error("Không lấy được dữ liệu từ TradingView")
                # Gửi cảnh báo Discord
                self.discord_alert.alert_no_data_from_source(
                    source="TradingView_LatestBars",
                    error_details=f"Không lấy được {n_bars} bars mới nhất",
                )
                return None, None

            # Format dữ liệu
            df = df.reset_index()

            # Tạo trường datetime
            df["datetime"] = pd.to_datetime(df["datetime"])

            # Đổi tên volume nếu cần
            if "volume" in df.columns:
                # Giữ nguyên nếu đã có tên đúng
                pass
            else:
                # Đổi tên từ vol sang volume nếu cần thiết
                df = df.rename(columns={"vol": "volume"})

            # Lấy thời điểm cũ nhất và mới nhất của dữ liệu
            oldest_datetime = df["datetime"].min()
            newest_datetime = df["datetime"].max()

            self.logger.info(f"Đã lấy được {len(df)} records từ TradingView")
            self.logger.info(f"Dữ liệu từ {oldest_datetime} đến {newest_datetime}")

            return df, oldest_datetime

        except Exception as e:
            self.logger.exception(f"Lỗi khi lấy dữ liệu từ TradingView: {e}")
            # Gửi cảnh báo Discord về exception
            self.discord_alert.alert_data_fetch_error(
                source="TradingView_LatestBars",
                error_message=f"Exception khi lấy {n_bars} bars: {str(e)}",
            )
            return None, None

    def maintain_latest_n_bars(self, n_bars=5000):
        """
        Duy trì chính xác n_bars mới nhất trong database
        và giữ lại dữ liệu cũ hơn thời điểm cũ nhất của n_bars

        Args:
            n_bars (int): Số lượng bars mới nhất cần duy trì

        Returns:
            DataFrame: DataFrame chứa dữ liệu mới cần thêm vào database
        """
        self.logger.info(f"Bắt đầu cập nhật và duy trì {n_bars} bars mới nhất")

        # Lấy dữ liệu mới nhất
        df, oldest_datetime = self.fetch_latest_n_bars(n_bars)

        if df is None or df.empty or oldest_datetime is None:
            self.logger.error("Không lấy được dữ liệu mới nhất từ TradingView")
            return pd.DataFrame()

        # Lấy danh sách datetime của n_bars mới
        new_datetimes = df["datetime"].tolist()

        # Xóa dữ liệu trong khoảng thời gian của n_bars mới nhưng không thuộc n_bars mới
        # Tìm số lượng records sẽ bị xóa (nếu có)
        now = datetime.now()
        count_to_delete = self.gold_collection.count_documents(
            {
                "datetime": {"$gte": oldest_datetime, "$lte": now},
                "datetime": {"$nin": new_datetimes},
            }
        )

        if count_to_delete > 0:
            self.logger.info(
                f"Sẽ xóa {count_to_delete} records cũ trong khoảng thời gian của {n_bars} bars mới"
            )
            # Thực hiện xóa nếu có records cần xóa
            result = self.gold_collection.delete_many(
                {
                    "datetime": {"$gte": oldest_datetime, "$lte": now},
                    "datetime": {"$nin": new_datetimes},
                }
            )
            self.logger.info(f"Đã xóa {result.deleted_count} records cũ")
        else:
            self.logger.info(
                f"Không có records cũ cần xóa trong khoảng thời gian của {n_bars} bars mới"
            )

        # Bây giờ kiểm tra xem records nào trong df đã tồn tại trong database
        return self.filter_existing_data(df)

    def filter_existing_data(self, df):
        """
        Lọc dữ liệu mới từ DataFrame, chỉ giữ lại các records chưa tồn tại trong DB

        Args:
            df (pandas.DataFrame): DataFrame chứa dữ liệu cần lọc

        Returns:
            pandas.DataFrame: DataFrame chỉ chứa dữ liệu mới
        """
        if df is None or df.empty:
            return df

        # Tạo danh sách các datetime để kiểm tra
        datetimes = df["datetime"].tolist()

        # Kiểm tra xem các records đã tồn tại chưa
        existing_records = list(
            self.gold_collection.find({"datetime": {"$in": datetimes}}, {"datetime": 1})
        )

        # Tạo set các datetime đã tồn tại để tìm kiếm nhanh
        existing_datetimes = set(record["datetime"] for record in existing_records)

        # Lọc chỉ giữ lại các records chưa tồn tại
        new_df = df[~df["datetime"].isin(existing_datetimes)]

        self.logger.info(
            f"Từ {len(df)} records, có {len(new_df)} records mới cần thêm vào database"
        )
        return new_df

    def realtime_extract(self, use_latest_n_bars=False, n_bars=5000):
        """
        Extract dữ liệu realtime: ưu tiên lấy các nến thiếu trước

        Args:
            use_latest_n_bars (bool): Nếu True, sẽ duy trì chính xác n_bars mới nhất
            n_bars (int): Số lượng bars mới nhất cần duy trì nếu use_latest_n_bars=True

        Returns:
            DataFrame: DataFrame chứa dữ liệu mới cần thêm vào database
        """
        self.logger.info("Extracting realtime metatrader data ...")

        # Phương thức mới: duy trì chính xác n_bars mới nhất
        if use_latest_n_bars:
            self.logger.info(f"Sử dụng phương thức duy trì {n_bars} bars mới nhất")
            return self.maintain_latest_n_bars(n_bars)

        # Phương thức cũ: kiểm tra và điền các khoảng trống dữ liệu
        # Kiểm tra dữ liệu thiếu trong 1 giờ gần đây (rút gọn thành 1 giờ thay vì 24 giờ để không ảnh hưởng hiệu suất)
        gap_df = self.check_and_fix_gaps(lookback_hours=1)
        if not gap_df.empty:
            # Lọc chỉ lấy dữ liệu mới
            filtered_gap_df = self.filter_existing_data(gap_df)
            if not filtered_gap_df.empty:
                self.logger.info(
                    f"Found {len(filtered_gap_df)} new missing records within the last hour"
                )
                return filtered_gap_df
            else:
                self.logger.info("No new records to add after filtering")

        # Nếu không có khoảng trống lớn, lấy các nến phút gần nhất đã hoàn thành
        missing_candles = self.get_missing_minute_candles()
        if not missing_candles.empty:
            # Lọc chỉ lấy dữ liệu mới
            filtered_missing_candles = self.filter_existing_data(missing_candles)
            if not filtered_missing_candles.empty:
                self.logger.info(
                    f"Extracted {len(filtered_missing_candles)} new missing completed candles"
                )
                return filtered_missing_candles
            else:
                self.logger.info("No new candles to add after filtering")
        else:
            self.logger.info("No missing candles found - data is up to date")

        return pd.DataFrame()
