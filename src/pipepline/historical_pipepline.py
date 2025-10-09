import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.etl.extract.historical_extract import HistoricalExtract
from src.etl.load.historical_load import HistoricalLoad


class HistoricalPipepline:
    def __init__(self):
        self.historical_extract = HistoricalExtract()
        self.historical_load = HistoricalLoad()

    def run(self):
        # Extract dữ liệu
        historical_data = self.historical_extract.historical_extract()
        # Load dữ liệu vào MongoDB
        self.historical_load.historical_load(historical_data)


if __name__ == "__main__":
    historical_pipepline = HistoricalPipepline()
    historical_pipepline.run()
