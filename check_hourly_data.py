#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script để kiểm tra phân bố dữ liệu theo giờ
"""

from datetime import datetime, timedelta
import pymongo

from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG
from config.logger_config import LoggerConfig

# Cấu hình logger
logger = LoggerConfig.logger_config("Check Hourly Data")


def check_hourly_data(target_date_str):
    """
    Kiểm tra dữ liệu theo giờ cho một ngày cụ thể

    Args:
        target_date_str (str): Ngày cần kiểm tra, định dạng 'YYYY-MM-DD'
    """
    try:
        # Parse ngày
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
        next_date = target_date + timedelta(days=1)

        logger.info(
            f"Kiểm tra dữ liệu theo giờ cho ngày {target_date.strftime('%Y-%m-%d')}"
        )

        # Kết nối MongoDB
        mongo_config = MongoConfig()
        mongo_client = mongo_config.get_client()
        gold_db = mongo_client.get_database(GOLD_DATA_CONFIG["database"])
        gold_collection = gold_db.get_collection(GOLD_DATA_CONFIG["collection"])

        # Đếm records theo giờ
        pipeline = [
            {"$match": {"datetime": {"$gte": target_date, "$lt": next_date}}},
            {
                "$group": {
                    "_id": {"hour": {"$hour": "$datetime"}},
                    "count": {"$sum": 1},
                    "first": {"$min": "$datetime"},
                    "last": {"$max": "$datetime"},
                }
            },
        ]

        result = list(gold_collection.aggregate(pipeline))

        if not result:
            logger.warning(f"Không tìm thấy dữ liệu cho ngày {target_date_str}")
            return

        # Hiển thị kết quả
        logger.info(f"Phân bố dữ liệu theo giờ cho ngày {target_date_str}:")

        # Sắp xếp theo giờ
        result.sort(key=lambda x: x["_id"]["hour"])

        for r in result:
            hour = r["_id"]["hour"]
            count = r["count"]
            first = r["first"]
            last = r["last"]

            # Tính số phút khoảng cách giữa record đầu và cuối
            time_diff_minutes = int((last - first).total_seconds() / 60) + 1

            # Expected count là 60 records mỗi giờ (1 record mỗi phút)
            expected_count = 60
            missing = expected_count - count

            if count < expected_count:
                status = f"THIẾU {missing} records"
            else:
                status = "OK"

            logger.info(
                f"Giờ {hour:02d}: {count}/{expected_count} records ({status}), từ {first.strftime('%H:%M:%S')} đến {last.strftime('%H:%M:%S')}, khoảng {time_diff_minutes} phút"
            )

            # Kiểm tra chi tiết hơn nếu thiếu records
            if count < expected_count and count > 0:
                # Lấy tất cả records trong giờ đó
                hour_start = datetime(
                    target_date.year, target_date.month, target_date.day, hour, 0, 0
                )
                hour_end = hour_start + timedelta(hours=1) - timedelta(seconds=1)

                minute_records = list(
                    gold_collection.find(
                        {"datetime": {"$gte": hour_start, "$lte": hour_end}}
                    ).sort("datetime", 1)
                )

                # Tạo set các phút đã có dữ liệu
                existing_minutes = set(r["datetime"].minute for r in minute_records)

                # Tìm các phút thiếu
                missing_minutes = [m for m in range(60) if m not in existing_minutes]

                logger.info(f"  Các phút thiếu dữ liệu: {missing_minutes}")

        # Kiểm tra các giờ hoàn toàn không có dữ liệu
        all_hours = set(range(24))
        existing_hours = set(r["_id"]["hour"] for r in result)
        missing_hours = all_hours - existing_hours

        if missing_hours:
            missing_hours_list = sorted(list(missing_hours))
            logger.warning(f"Các giờ hoàn toàn không có dữ liệu: {missing_hours_list}")

    except Exception as e:
        logger.exception(f"Lỗi khi kiểm tra dữ liệu theo giờ: {e}")


def main():
    import sys

    if len(sys.argv) != 2:
        print("Usage: python check_hourly_data.py <date>")
        print("Example: python check_hourly_data.py 2025-10-13")
        return 1

    target_date = sys.argv[1]
    check_hourly_data(target_date)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
