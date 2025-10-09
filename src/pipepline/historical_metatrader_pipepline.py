import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.etl.extract.historical_metatrader_extract import HistoricalMetatraderExtract
from src.etl.load.historical_metatrader_load import HistoricalMetatraderLoad


class HistoricalMetatraderPipepline:
    def __init__(self):
        self.extractor = HistoricalMetatraderExtract()
        self.loader = HistoricalMetatraderLoad()

    def run(self):
        # Extract dữ liệu
        metatrader_data = self.extractor.historical_extract()
        # Load dữ liệu vào MongoDB
        self.loader.historical_load(metatrader_data)


if __name__ == "__main__":
    pipepline = HistoricalMetatraderPipepline()
    pipepline.run()
