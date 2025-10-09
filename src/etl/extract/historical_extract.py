import kagglehub
from kagglehub import KaggleDatasetAdapter
from config.logger_config import LoggerConfig
from config.variable_config import GOLD_DATA_CONFIG


class HistoricalExtract:
    def __init__(self) -> None:
        try:
            self.logger = LoggerConfig.logger_config("Extract Historical gold data")
            self.historical_data_url_from_kaggle = GOLD_DATA_CONFIG[
                "historical_data_url_from_kaggle"
            ]
            self.historical_data_url_from_kaggle_file_path = GOLD_DATA_CONFIG[
                "historical_data_url_from_kaggle_file_path"
            ]
            self.logger.info("Successfully to read config")
        except Exception as e:
            self.logger.error(f"Error to read config: {str(e)}")

    def historical_extract(self):
        try:
            self.logger.info("Extract historical gold data ...")
            historical_data_extract = kagglehub.load_dataset(
                KaggleDatasetAdapter.PANDAS,
                self.historical_data_url_from_kaggle,
                self.historical_data_url_from_kaggle_file_path,
            )
            if "Date;Open;High;Low;Close;Volume" in historical_data_extract.columns:
                historical_data_extract = historical_data_extract[
                    "Date;Open;High;Low;Close;Volume"
                ].str.split(";", expand=True)
                historical_data_extract.columns = [
                    "Date",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                ]
            self.logger.info(
                f"Extracted data successfully: {len(historical_data_extract)} records"
            )
            return historical_data_extract
        except Exception as e:
            self.logger.error(f"Error to extract data: {str(e)}")
            raise
