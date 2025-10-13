import sys
import os
import schedule
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.etl.extract.realtime_metatrader_extract import RealtimeMetatraderExtract
from src.etl.load.realtime_metatrader_load import RealtimeMetatraderLoad


class RealtimeMetatraderPipepline:
    def __init__(self):
        self.extractor = RealtimeMetatraderExtract()
        self.loader = RealtimeMetatraderLoad()
        # Lưu phút cuối cùng đã cập nhật
        self.last_updated_minute = None

    def run_once(self):
        """Chạy 1 lần để lấy dữ liệu mới (các nến đã hoàn thành)"""
        df = self.extractor.realtime_extract()
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

    def run_realtime(self):
        """Chạy pipeline realtime với logic ưu tiên:
        1. Mỗi phút: Lấy các nến thiếu (ưu tiên cao)
        2. Mỗi 5 giây:
           - Cập nhật trạng thái cuối cùng của nến phút trước (nếu vừa chuyển phút)
           - Upsert nến hiện tại (chỉ khi data đã đủ)
        """
        schedule.every(1).minutes.do(self.run_once)
        schedule.every(5).seconds.do(self.upsert_current_minute)

        print("Realtime pipeline started:")
        print("- Every 1 minute: Fetch missing completed candles (priority)")
        print(
            "- Every 5 seconds: Update previous minute's final state (if just changed)"
        )
        print(
            "- Every 5 seconds: Update current minute candle (only when data is up-to-date)"
        )
        print("Press Ctrl+C to stop.")

        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    pipepline = RealtimeMetatraderPipepline()
    pipepline.run_realtime()
