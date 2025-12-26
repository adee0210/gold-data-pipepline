import sys
import os
import schedule
import time
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.etl.extract.realtime_metatrader_extract import RealtimeMetatraderExtract
from src.etl.load.realtime_metatrader_load import RealtimeMetatraderLoad


class RealtimeMetatraderPipepline:
    def __init__(self):
        self.extractor = RealtimeMetatraderExtract()
        self.loader = RealtimeMetatraderLoad()
        self.last_minute = None

    def upsert_current_minute(self):
        """Upsert nến phút hiện tại mỗi 2 giây, update phút cũ khi chuyển phút"""
        try:
            current_minute = datetime.now().replace(second=0, microsecond=0)

            # Nếu vừa chuyển phút mới, update lại phút cũ lần cuối
            if self.last_minute and self.last_minute != current_minute:
                try:
                    df_prev = self.extractor.get_specific_minute_candle(
                        self.last_minute
                    )
                    if not df_prev.empty:
                        self.loader.upsert_current_minute_candle(df_prev)
                        print(f"Updated previous minute {self.last_minute} final state")
                except Exception as e:
                    print(f"Error updating previous minute: {e}")
                    # Không raise, tiếp tục với current minute

            # Upsert phút hiện tại
            df = self.extractor.get_current_minute_candle()
            if not df.empty:
                self.loader.upsert_current_minute_candle(df)
                self.last_minute = current_minute
        except Exception as e:
            print(f"Error in upsert_current_minute: {e}")
            # Không raise, để loop tiếp tục chạy

    def check_and_fix_gaps(self, lookback_hours=24):
        """Kiểm tra và bù dữ liệu thiếu trong N giờ gần nhất"""
        try:
            gap_df = self.extractor.check_and_fix_gaps(lookback_hours=lookback_hours)
            if not gap_df.empty:
                self.loader.realtime_load(gap_df)
                print(
                    f"Fixed {len(gap_df)} missing records in the last {lookback_hours} hours"
                )
            else:
                print(f"No data gaps found in the last {lookback_hours} hours")
        except Exception as e:
            print(f"Error checking/fixing gaps: {e}")
            # Không raise, để pipeline vẫn start được

    def run_realtime(self):
        # Bù dữ liệu thiếu khi khởi động (chỉ chạy 1 lần)
        print("Checking for historical data gaps on startup...")
        self.check_and_fix_gaps(lookback_hours=24)

        # Schedule: upsert nến hiện tại mỗi 2 giây
        schedule.every(2).seconds.do(self.upsert_current_minute)

        print("Realtime pipeline started:")
        print("- Every 2 seconds: Upsert current minute candle")
        print("- Auto update previous minute when time changes")
        print("Press Ctrl+C to stop.")

        consecutive_errors = 0
        max_consecutive_errors = 10  # Restart sau 10 lỗi liên tiếp

        try:
            while True:
                try:
                    schedule.run_pending()
                    consecutive_errors = 0  # Reset counter khi thành công
                    time.sleep(1)
                except KeyboardInterrupt:
                    print("Received shutdown signal. Exiting...")
                    sys.exit(0)
                except Exception as e:
                    consecutive_errors += 1
                    print(
                        f"Unexpected error in main loop (error {consecutive_errors}/{max_consecutive_errors}): {e}"
                    )
                    import traceback

                    traceback.print_exc()

                    if consecutive_errors >= max_consecutive_errors:
                        print(
                            f"Too many consecutive errors ({consecutive_errors}), restarting pipeline..."
                        )
                        # Reset schedule và restart
                        schedule.clear()
                        consecutive_errors = 0
                        self.check_and_fix_gaps(
                            lookback_hours=1
                        )  # Kiểm tra gaps ngắn hơn
                        schedule.every(2).seconds.do(self.upsert_current_minute)
                        print("Pipeline restarted after errors")
                    else:
                        time.sleep(5)  # Chờ 5 giây trước khi tiếp tục
        except KeyboardInterrupt:
            print("Received shutdown signal. Exiting...")
            sys.exit(0)


if __name__ == "__main__":
    pipeline = RealtimeMetatraderPipepline()
    pipeline.run_realtime()
