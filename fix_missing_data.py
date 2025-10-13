#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script để sửa lỗi thiếu dữ liệu realtime bằng cách cải tiến logic kiểm tra
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import time
import argparse
from pymongo.errors import BulkWriteError

# Thêm thư mục gốc vào sys.path để import các module trong project
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from config.logger_config import LoggerConfig
from src.etl.extract.realtime_metatrader_extract import RealtimeMetatraderExtract
from src.etl.load.realtime_metatrader_load import RealtimeMetatraderLoad

# Cấu hình logger
logger = LoggerConfig.logger_config("Fix Missing Data")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Sửa lỗi và cập nhật dữ liệu thiếu")
    parser.add_argument(
        "--start_date",
        type=str,
        required=True,
        help="Ngày bắt đầu kiểm tra, định dạng YYYY-MM-DD",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        help="Ngày kết thúc kiểm tra, định dạng YYYY-MM-DD (mặc định là ngày hiện tại)",
    )
    parser.add_argument(
        "--dry_run", action="store_true", help="Chỉ kiểm tra không cập nhật dữ liệu"
    )

    return parser.parse_args()


def get_missing_time_ranges(extractor, start_date_str, end_date_str=None):
    """
    Lấy danh sách các khoảng thời gian bị thiếu dữ liệu

    Args:
        extractor: RealtimeMetatraderExtract instance
        start_date_str (str): Ngày bắt đầu, định dạng YYYY-MM-DD
        end_date_str (str, optional): Ngày kết thúc, định dạng YYYY-MM-DD

    Returns:
        list: Danh sách các tuple (start_time, end_time) bị thiếu dữ liệu
    """
    try:
        # Parse thời gian
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        if end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1)
        else:
            end_date = datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=1)

        logger.info(f"Kiểm tra dữ liệu thiếu từ {start_date} đến {end_date}")

        # Kết nối MongoDB qua extractor
        collection = extractor.gold_collection

        # Lấy tất cả records trong khoảng thời gian
        records = list(
            collection.find({"datetime": {"$gte": start_date, "$lt": end_date}}).sort(
                "datetime", 1
            )
        )

        if not records:
            logger.warning(f"Không có dữ liệu nào trong khoảng thời gian này!")
            # Trả về toàn bộ khoảng thời gian
            return [(start_date, end_date - timedelta(minutes=1))]

        # Tìm các khoảng thời gian thiếu
        missing_ranges = []

        # Bắt đầu kiểm tra từ start_date
        current_time = start_date

        for record in records:
            record_time = record["datetime"]

            # Nếu record_time > current_time + 1 phút, tức là có khoảng trống
            if record_time > current_time + timedelta(minutes=1):
                # Có khoảng trống từ current_time đến record_time - 1 phút
                missing_ranges.append(
                    (current_time, record_time - timedelta(minutes=1))
                )
                logger.info(
                    f"Tìm thấy khoảng trống: {current_time} đến {record_time - timedelta(minutes=1)}"
                )

            # Cập nhật current_time
            current_time = record_time + timedelta(minutes=1)

        # Kiểm tra khoảng trống cuối cùng (từ record cuối đến end_date)
        last_record_time = records[-1]["datetime"]
        if end_date > last_record_time + timedelta(minutes=1):
            missing_ranges.append(
                (
                    last_record_time + timedelta(minutes=1),
                    end_date - timedelta(minutes=1),
                )
            )
            logger.info(
                f"Tìm thấy khoảng trống cuối: {last_record_time + timedelta(minutes=1)} đến {end_date - timedelta(minutes=1)}"
            )

        # Tóm tắt kết quả
        if missing_ranges:
            total_missing_minutes = sum(
                (end - start).total_seconds() // 60 + 1 for start, end in missing_ranges
            )
            logger.info(
                f"Tổng cộng có {len(missing_ranges)} khoảng trống với khoảng {int(total_missing_minutes)} phút dữ liệu bị thiếu"
            )
        else:
            logger.info("Không tìm thấy khoảng trống dữ liệu")

        return missing_ranges

    except Exception as e:
        logger.exception(f"Lỗi khi tìm khoảng thời gian thiếu: {e}")
        return []


def fetch_and_update_missing_data(
    extractor, loader, start_time, end_time, dry_run=False
):
    """
    Lấy và cập nhật dữ liệu thiếu cho một khoảng thời gian

    Args:
        extractor: RealtimeMetatraderExtract instance
        loader: RealtimeMetatraderLoad instance
        start_time (datetime): Thời điểm bắt đầu
        end_time (datetime): Thời điểm kết thúc
        dry_run (bool): Nếu True, chỉ lấy dữ liệu không cập nhật

    Returns:
        int: Số lượng records đã cập nhật
    """
    try:
        logger.info(f"Đang lấy dữ liệu từ {start_time} đến {end_time}")

        # Tính khoảng thời gian tính theo phút
        time_range_minutes = int((end_time - start_time).total_seconds() // 60) + 1

        if time_range_minutes > 5000:
            # Chia nhỏ khoảng thời gian nếu lớn hơn 5000 phút
            logger.info(
                f"Khoảng thời gian quá lớn ({time_range_minutes} phút), chia thành nhiều lần lấy dữ liệu"
            )

            current_start = start_time
            total_updated = 0

            while current_start <= end_time:
                current_end = min(current_start + timedelta(minutes=4999), end_time)
                updated = fetch_and_update_missing_data(
                    extractor, loader, current_start, current_end, dry_run
                )
                total_updated += updated
                current_start = current_end + timedelta(minutes=1)

                # Đợi một chút để tránh quá tải API
                time.sleep(2)

            return total_updated

        # Lấy dữ liệu từ TradingView
        from tvDatafeed import TvDatafeed, Interval

        tv = TvDatafeed()

        # Dùng from_date để lấy dữ liệu từ start_time
        df = tv.get_hist(
            symbol="XAUUSD",
            exchange="OANDA",
            interval=Interval.in_1_minute,
            n_bars=time_range_minutes,
        )

        if df is None or df.empty:
            logger.warning(
                f"Không lấy được dữ liệu từ TradingView cho khoảng {start_time} đến {end_time}"
            )
            return 0

        # Format lại dữ liệu
        df = df.reset_index()

        # Tạo trường datetime
        df = df.rename(columns={"datetime": "datetime", "volume": "volume"})

        # Filter chỉ lấy dữ liệu trong khoảng thời gian cần thiết
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df[(df["datetime"] >= start_time) & (df["datetime"] <= end_time)]

        if df.empty:
            logger.warning(
                f"Sau khi filter, không còn dữ liệu nào trong khoảng {start_time} đến {end_time}"
            )
            return 0

        logger.info(f"Đã lấy được {len(df)} records từ TradingView")

        if dry_run:
            logger.info(f"DRY RUN: Sẽ cập nhật {len(df)} records vào database")
            return len(df)

        # Cập nhật vào database
        try:
            loader.realtime_load(df)
            records_count = len(df)
            logger.info(f"Đã cập nhật {records_count} records vào database")
            return records_count
        except Exception as e:
            logger.exception(f"Lỗi khi cập nhật dữ liệu: {e}")
            return 0

    except Exception as e:
        logger.exception(f"Lỗi khi lấy và cập nhật dữ liệu: {e}")
        return 0


def main():
    """Main function"""
    args = parse_arguments()

    # Khởi tạo extractor và loader
    extractor = RealtimeMetatraderExtract()
    loader = RealtimeMetatraderLoad()

    # Lấy danh sách khoảng thời gian thiếu dữ liệu
    missing_ranges = get_missing_time_ranges(extractor, args.start_date, args.end_date)

    if not missing_ranges:
        logger.info("Không có khoảng thời gian nào cần cập nhật")
        return 0

    # Cập nhật từng khoảng thời gian
    total_updated = 0
    for start_time, end_time in missing_ranges:
        updated = fetch_and_update_missing_data(
            extractor, loader, start_time, end_time, args.dry_run
        )
        total_updated += updated

    logger.info(f"Tổng cộng đã cập nhật {total_updated} records")
    return 0


if __name__ == "__main__":
    sys.exit(main())
