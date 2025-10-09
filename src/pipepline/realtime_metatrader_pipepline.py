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

    def run_once(self):
        """Chạy 1 lần để lấy dữ liệu mới (các nến đã hoàn thành)"""
        df = self.extractor.realtime_extract()
        self.loader.realtime_load(df)

    def upsert_current_minute(self):
        """Upsert nến phút hiện tại (đang hình thành)"""
        df = self.extractor.get_current_minute_candle()
        self.loader.upsert_current_minute_candle(df)

    def run_realtime(self):
        """Chạy pipeline realtime với 2 tác vụ:
        1. Mỗi phút: Lấy dữ liệu mới (nến đã hoàn thành)
        2. Mỗi 5 giây: Upsert nến phút hiện tại
        """
        schedule.every(1).minutes.do(self.run_once)
        schedule.every(5).seconds.do(self.upsert_current_minute)

        print("Realtime pipeline started:")
        print("- Every 1 minute: Fetch new completed candles")
        print("- Every 5 seconds: Update current minute candle")
        print("Press Ctrl+C to stop.")

        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    pipepline = RealtimeMetatraderPipepline()
    pipepline.run_realtime()
