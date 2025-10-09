import gdown
import pandas as pd
from config.logger_config import LoggerConfig
from config.variable_config import GOLD_DATA_CONFIG
import os


class HistoricalMetatraderExtract:
    def __init__(self) -> None:
        try:
            self.logger = LoggerConfig.logger_config(
                "Extract Historical Metatrader gold data"
            )
            self.gdrive_url = GOLD_DATA_CONFIG["metatrader_data_gdrive_url"]
            self.logger.info("Successfully to read config")
        except Exception as e:
            self.logger.error(f"Error to read config: {str(e)}")

    def historical_extract(self):
        try:
            self.logger.info("Downloading Metatrader data from Google Drive ...")
            temp_path = "/tmp/metatrader_data.csv"
            gdown.download(self.gdrive_url, temp_path, quiet=True)
            df = pd.read_csv(temp_path, sep="\t", engine="python")
            # Đổi tên cột về dạng thường
            df.columns = [
                c.lower()
                for c in [
                    "DATE",
                    "TIME",
                    "OPEN",
                    "HIGH",
                    "LOW",
                    "CLOSE",
                    "TICKVOL",
                    "VOL",
                    "SPREAD",
                ]
            ]
            self.logger.info(f"Extracted data successfully: {len(df)} records")
            return df
        except Exception as e:
            self.logger.error(f"Error to extract data: {str(e)}")
            raise
