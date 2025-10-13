#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script để kiểm tra dữ liệu mới nhất trong collection gold_minute_data
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd
from pymongo import MongoClient, ASCENDING, DESCENDING

# Thêm thư mục gốc vào sys.path để import các module trong project
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG

# Cấu hình logger
logger = LoggerConfig.logger_config("Check Last Gold Data")


def check_database_status():
    """
    Kiểm tra trạng thái của database: dữ liệu mới nhất, số lượng records, khoảng thời gian
    """
    try:
        # Kết nối MongoDB
        mongo_config = MongoConfig()
        mongo_client = mongo_config.get_client()
        gold_db = mongo_client.get_database(GOLD_DATA_CONFIG["database"])
        gold_collection = gold_db.get_collection(GOLD_DATA_CONFIG["collection"])

        # Lấy số lượng records
        total_records = gold_collection.count_documents({})
        logger.info(f"Tổng số records: {total_records:,}")

        # Lấy record mới nhất
        newest_record = gold_collection.find_one({}, sort=[("datetime", DESCENDING)])
        if newest_record:
            newest_time = newest_record["datetime"]
            logger.info(f"Dữ liệu mới nhất: {newest_time}")

            # So sánh với thời gian hiện tại
            now = datetime.now()
            time_diff = now - newest_time
            logger.info(f"Dữ liệu cũ hơn hiện tại: {time_diff}")
        else:
            logger.warning("Không tìm thấy dữ liệu trong collection")
            return

        # Lấy record cũ nhất
        oldest_record = gold_collection.find_one({}, sort=[("datetime", ASCENDING)])
        if oldest_record:
            oldest_time = oldest_record["datetime"]
            logger.info(f"Dữ liệu cũ nhất: {oldest_time}")

            # Tính khoảng thời gian có dữ liệu
            time_range = newest_time - oldest_time
            logger.info(f"Khoảng thời gian có dữ liệu: {time_range}")

        # Kiểm tra dữ liệu từ ngày 7/10/2025
        target_date = datetime(2025, 10, 7, 0, 0, 0)
        next_date = datetime(2025, 10, 8, 0, 0, 0)

        # Đếm records cho ngày 7/10/2025
        records_count = gold_collection.count_documents(
            {"datetime": {"$gte": target_date, "$lt": next_date}}
        )
        logger.info(f"Số lượng records ngày 7/10/2025: {records_count:,}")

        # Hiển thị 5 records đầu tiên của ngày 7/10/2025
        first_records = list(
            gold_collection.find(
                {
                    "datetime": {
                        "$gte": target_date,
                        "$lt": target_date + timedelta(hours=1),
                    }
                },
                sort=[("datetime", ASCENDING)],
                limit=5,
            )
        )

        if first_records:
            logger.info("5 records đầu tiên của ngày 7/10/2025:")
            for record in first_records:
                logger.info(
                    f"{record['datetime']} - Open: {record['open']}, Close: {record['close']}, Volume: {record.get('volume', 'N/A')}"
                )
        else:
            logger.warning("Không tìm thấy dữ liệu cho ngày 7/10/2025")

        return {
            "total_records": total_records,
            "newest_time": newest_time if newest_record else None,
            "oldest_time": oldest_time if oldest_record else None,
            "target_date_records": records_count,
        }

    except Exception as e:
        logger.exception(f"Lỗi khi kiểm tra database: {e}")
        return None


def main():
    """Main function"""
    logger.info("Bắt đầu kiểm tra dữ liệu gold")
    status = check_database_status()

    if status:
        logger.info("Kết quả kiểm tra:")
        for key, value in status.items():
            logger.info(f"- {key}: {value}")

    logger.info("Kiểm tra dữ liệu hoàn tất")
    return 0


if __name__ == "__main__":
    sys.exit(main())
