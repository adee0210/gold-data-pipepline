import sys
import os
import schedule
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.etl.extract.realtime_metatrader_extract import RealtimeMetatraderExtract
from src.etl.load.realtime_metatrader_load import RealtimeMetatraderLoad


class RealtimeMetatraderPipepline:
    def __init__(self, use_latest_n_bars=False, n_bars=5000):
        self.extractor = RealtimeMetatraderExtract()
        self.loader = RealtimeMetatraderLoad()
        # Lưu phút cuối cùng đã cập nhật
        self.last_updated_minute = None
        self.use_latest_n_bars = use_latest_n_bars
        self.n_bars = n_bars

    def run_once(self):
        """Chạy 1 lần để lấy dữ liệu mới (các nến đã hoàn thành)"""
        df = self.extractor.realtime_extract(
            use_latest_n_bars=self.use_latest_n_bars, n_bars=self.n_bars
        )
        self.loader.realtime_load(df)

    def update_previous_minute_final_state(self):
        """Cập nhật trạng thái cuối cùng của nến phút trước"""
        from datetime import datetime

        # Lấy phút hiện tại
        now = datetime.now()
        current_minute = now.replace(second=0, microsecond=0)

        # Kiểm tra xem đã chuyển phút hay chưa
        if (
            self.last_updated_minute is None
            or self.last_updated_minute != current_minute
        ):
            # Cập nhật nến phút trước khi chuyển sang phút mới
            previous_candle = self.extractor.get_previous_minute_final_candle()
            if not previous_candle.empty:
                self.loader.upsert_current_minute_candle(previous_candle)
                print(f"Updated final state of previous minute candle")

            # Ghi nhớ phút hiện tại đã cập nhật
            self.last_updated_minute = current_minute

    def upsert_current_minute(self):
        """Upsert nến phút hiện tại (đang hình thành) - chỉ khi data đã up-to-date"""
        # Cập nhật nến phút trước nếu vừa chuyển phút
        self.update_previous_minute_final_state()

        # Chỉ upsert nến hiện tại khi không còn data thiếu
        if self.extractor.is_data_up_to_date():
            df = self.extractor.get_current_minute_candle()
            self.loader.upsert_current_minute_candle(df)
        else:
            print("Data not up-to-date, skipping current minute upsert")

    def check_and_fix_historical_gaps(self, lookback_hours=24):
        """
        Kiểm tra và sửa chữa khoảng trống dữ liệu trong lịch sử

        Args:
            lookback_hours (int): Số giờ cần kiểm tra ngược về quá khứ
        """
        # Sử dụng phương thức mới trong extractor để lấy dữ liệu thiếu
        gap_df = self.extractor.check_and_fix_gaps(lookback_hours=lookback_hours)

        if not gap_df.empty:
            # Load dữ liệu vào database
            self.loader.realtime_load(gap_df)
            print(
                f"Fixed {len(gap_df)} missing records in the last {lookback_hours} hours"
            )
        else:
            print(f"No data gaps found in the last {lookback_hours} hours")

    def run_realtime(self):
        # Kiểm tra và sửa dữ liệu thiếu khi khởi động
        if self.use_latest_n_bars:
            print(f"Maintaining exactly {self.n_bars} latest bars on startup...")
            df = self.extractor.maintain_latest_n_bars(n_bars=self.n_bars)
            self.loader.realtime_load(df)
        else:
            print("Checking for historical data gaps on startup...")
            self.check_and_fix_historical_gaps(lookback_hours=24)

        schedule.every(1).minutes.do(self.run_once)
        schedule.every(5).seconds.do(self.upsert_current_minute)

        if self.use_latest_n_bars:
            # Duy trì đúng n_bars mới nhất mỗi 4 giờ
            schedule.every(4).hours.do(lambda: self.run_once())
            print(f"Running in maintain-latest-{self.n_bars}-bars mode")
        else:
            # Kiểm tra và sửa khoảng trống dữ liệu mỗi 4 giờ
            schedule.every(4).hours.do(
                lambda: self.check_and_fix_historical_gaps(lookback_hours=24)
            )

        print("Realtime pipeline started with enhanced gap detection:")
        print("- Every 1 minute: Fetch missing completed candles (priority)")
        print(
            "- Every 5 seconds: Update previous minute's final state (if just changed)"
        )
        print(
            "- Every 5 seconds: Update current minute candle (only when data is up-to-date)"
        )
        print("- Every 4 hours: Check and fix historical data gaps (last 24 hours)")
        print("Press Ctrl+C to stop.")

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("Received shutdown signal. Exiting...")
            sys.exit(0)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run realtime MetaTrader data pipeline"
    )
    parser.add_argument(
        "--maintain-latest", action="store_true", help="Maintain exactly N latest bars"
    )
    parser.add_argument(
        "--n-bars",
        type=int,
        default=5000,
        help="Number of latest bars to maintain (default: 5000)",
    )

    args = parser.parse_args()

    pipepline = RealtimeMetatraderPipepline(
        use_latest_n_bars=args.maintain_latest, n_bars=args.n_bars
    )

    pipepline.run_realtime()
