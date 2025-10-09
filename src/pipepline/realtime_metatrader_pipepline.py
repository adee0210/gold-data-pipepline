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
        df = self.extractor.realtime_extract()
        self.loader.realtime_load(df)

    def run_realtime(self):
        schedule.every(1).minutes.do(self.run_once)
        print("Realtime pipeline started. Press Ctrl+C to stop.")
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    pipepline = RealtimeMetatraderPipepline()
    pipepline.run_realtime()
