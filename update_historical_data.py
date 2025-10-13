#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script để cập nhật dữ liệu lịch sử gold từ ngày 07/10/2023
Tải và cập nhật 5000 records mới nhất vào database
"""

import sys
import os
import pandas as pd
from datetime import datetime
import argparse
from tvDatafeed import TvDatafeed, Interval
import pymongo
from pymongo.errors import BulkWriteError
import logging

# Thêm thư mục gốc vào sys.path để import các module trong project
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG

# Cấu hình logger
logger = LoggerConfig.logger_config("Update Historical Gold Data")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Update historical gold data from TradingView"
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default="2023-10-07",
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--records", type=int, default=5000, help="Number of records to fetch"
    )
    parser.add_argument(
        "--dry_run", action="store_true", help="Run without updating database"
    )

    return parser.parse_args()


def get_historical_data(start_date, n_bars=5000):
    """
    Lấy dữ liệu lịch sử từ TradingView

    Args:
        start_date (str): Ngày bắt đầu dữ liệu (YYYY-MM-DD)
        n_bars (int): Số lượng records cần lấy

    Returns:
        pandas.DataFrame: DataFrame chứa dữ liệu gold
    """
    try:
        logger.info(
            f"Fetching {n_bars} records of historical gold data from TradingView starting from {start_date}"
        )

        # Khởi tạo TvDatafeed adapter
        tv = TvDatafeed()

        # Lấy dữ liệu từ TradingView (sử dụng OANDA thay vì FOREXCOM để có volume đúng)
        # Đối tượng tvDatafeed không có tham số from_date
        # Đầu tiên chúng ta lấy số lượng records yêu cầu
        df = tv.get_hist(
            symbol="XAUUSD",
            exchange="OANDA",
            interval=Interval.in_1_minute,
            n_bars=n_bars,
        )

        # Sau đó filter theo ngày
        if df is not None and not df.empty:
            start_datetime = pd.to_datetime(start_date)
            df = df[df.index >= start_datetime]

        if df is None or df.empty:
            logger.error("No data returned from TradingView")
            return None

        # Format lại dữ liệu để khớp với schema
        df = df.reset_index()

        # Tạo trường datetime
        df = df.rename(columns={"datetime": "datetime", "volume": "volume"})

        # Đảm bảo trường datetime là datetime object
        df["datetime"] = pd.to_datetime(df["datetime"])

        logger.info(f"Successfully fetched {len(df)} records from TradingView")
        return df

    except Exception as e:
        logger.exception(f"Error fetching historical data: {e}")
        return None


def update_database(data_df, dry_run=False):
    """
    Cập nhật dữ liệu vào MongoDB

    Args:
        data_df (pandas.DataFrame): DataFrame chứa dữ liệu gold
        dry_run (bool): Nếu True, chỉ hiển thị kết quả không cập nhật database

    Returns:
        bool: True nếu cập nhật thành công, False nếu có lỗi
    """
    if dry_run:
        logger.info(f"DRY RUN: Would update {len(data_df)} records to database")
        logger.info(f"Sample data: {data_df.head(2).to_dict('records')}")
        return True

    try:
        # Kết nối MongoDB
        mongo_config = MongoConfig()
        mongo_client = mongo_config.get_client()
        gold_db = mongo_client.get_database(GOLD_DATA_CONFIG["database"])
        gold_collection = gold_db.get_collection(GOLD_DATA_CONFIG["collection"])

        # Đảm bảo có index trên trường datetime
        gold_collection.create_index([("datetime", 1)], unique=True, background=True)

        # Chuyển DataFrame thành list of dicts
        records = data_df.to_dict("records")

        # Batch size để insert
        batch_size = 1000
        batches = [
            records[i : i + batch_size] for i in range(0, len(records), batch_size)
        ]

        total_inserted = 0
        total_duplicates = 0
        total_errors = 0

        # Insert từng batch
        for i, batch in enumerate(batches):
            try:
                # Sử dụng upsert để cập nhật nếu đã tồn tại
                operations = [
                    pymongo.UpdateOne(
                        {"datetime": record["datetime"]}, {"$set": record}, upsert=True
                    )
                    for record in batch
                ]

                result = gold_collection.bulk_write(operations, ordered=False)
                batch_inserted = result.upserted_count + result.modified_count
                total_inserted += batch_inserted

                logger.info(
                    f"Batch {i+1}: Inserted/Updated {batch_inserted}/{len(batch)} records"
                )

            except BulkWriteError as bwe:
                details = bwe.details or {}
                nUpserted = details.get("nUpserted", 0)
                nModified = details.get("nModified", 0)
                writeErrors = details.get("writeErrors", [])
                dup_count = sum(1 for we in writeErrors if we.get("code") == 11000)
                other_errors = [we for we in writeErrors if we.get("code") != 11000]

                total_inserted += nUpserted + nModified
                total_duplicates += dup_count
                total_errors += len(other_errors)

                logger.info(
                    f"Batch {i+1} partial insert: {nUpserted + nModified}/{len(batch)} inserted/updated, "
                    f"duplicates: {dup_count}, other errors: {len(other_errors)}"
                )

                if other_errors:
                    logger.error(
                        f"Non-duplicate write errors in batch {i+1}: {other_errors[0]}"
                    )

            except Exception as e:
                logger.exception(f"Error updating batch {i+1}: {e}")
                total_errors += len(batch)

        logger.info(
            f"Database update completed. Total records inserted/updated: {total_inserted}"
        )
        logger.info(f"Duplicates: {total_duplicates}, Other errors: {total_errors}")

        return True

    except Exception as e:
        logger.exception(f"Error updating database: {e}")
        return False


def main():
    """Main function to run the update process"""
    args = parse_arguments()

    # Lấy dữ liệu từ TradingView
    df = get_historical_data(args.start_date, args.records)

    if df is None:
        logger.error("Failed to fetch data. Exiting.")
        return 1

    logger.info(f"Data range: {df['datetime'].min()} to {df['datetime'].max()}")

    # Cập nhật database
    success = update_database(df, args.dry_run)

    if not success:
        logger.error("Database update failed.")
        return 1

    logger.info("Update process completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
