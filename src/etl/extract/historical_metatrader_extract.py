import gdown
import pandas as pd
from config.variable_config import GOLD_DATA_CONFIG
import os
import logging
from config.logger_config import LoggerConfig

logger = LoggerConfig.logger_config(__name__)


class HistoricalMetatraderExtract:
    def __init__(self) -> None:
        try:
            self.gdrive_url = GOLD_DATA_CONFIG["metatrader_data_gdrive_url"]
            logger.info("Đã đọc cấu hình thành công")
        except Exception as e:
            logger.error(f"Không thể đọc cấu hình: {e}")

    def historical_extract(self):
        try:
            logger.info("Đang tải dữ liệu Metatrader từ Google Drive...")
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

            # Kết hợp date và time thành datetime field duy nhất
            df["datetime"] = pd.to_datetime(
                df["date"] + " " + df["time"], format="%Y.%m.%d %H:%M:%S"
            )

            # Đổi tên tickvol thành volume và xóa các cột không cần thiết
            df = df.rename(columns={"tickvol": "volume"})
            df = df.drop(columns=["date", "time", "vol", "spread"])

            logger.info(f"Đã trích xuất dữ liệu thành công: {len(df)} bản ghi")
            return df
        except Exception as e:
            logger.exception(f"Lỗi khi trích xuất dữ liệu: {e}")
            return None
