#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script để kiểm tra khoảng trống trong dữ liệu gold
"""

import sys
from datetime import datetime, timedelta
from pymongo import MongoClient

from config.mongo_config import MongoConfig
from config.variable_config import GOLD_DATA_CONFIG
from config.logger_config import LoggerConfig

# Cấu hình logger
logger = LoggerConfig.logger_config("Check Data Gaps")

def check_data_gaps(start_time_str, end_time_str):
    """
    Kiểm tra khoảng trống dữ liệu trong một khoảng thời gian
    
    Args:
        start_time_str (str): Thời gian bắt đầu dạng 'YYYY-MM-DD HH:MM:SS'
        end_time_str (str): Thời gian kết thúc dạng 'YYYY-MM-DD HH:MM:SS'
    """
    try:
        # Parse thời gian
        start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Kiểm tra khoảng trống từ {start_time} đến {end_time}")
        
        # Kết nối MongoDB
        mongo_config = MongoConfig()
        mongo_client = mongo_config.get_client()
        gold_db = mongo_client.get_database(GOLD_DATA_CONFIG["database"])
        gold_collection = gold_db.get_collection(GOLD_DATA_CONFIG["collection"])
        
        # Lấy records trong khoảng thời gian
        records = list(gold_collection.find({
            "datetime": {"$gte": start_time, "$lte": end_time}
        }).sort("datetime", 1))
        
        if not records:
            logger.warning(f"Không tìm thấy dữ liệu nào trong khoảng thời gian này")
            return
            
        logger.info(f"Tìm thấy {len(records)} records từ {start_time} đến {end_time}")
        
        # Kiểm tra khoảng trống
        gap_count = 0
        times = [r['datetime'] for r in records]
        
        # Hiện thị 5 record đầu và cuối
        logger.info("5 record đầu tiên:")
        for i in range(min(5, len(records))):
            r = records[i]
            logger.info(f"{r['datetime']} - Open: {r['open']}, Close: {r['close']}, Volume: {r.get('volume', 'N/A')}")
            
        logger.info("5 record cuối cùng:")
        for i in range(max(0, len(records)-5), len(records)):
            r = records[i]
            logger.info(f"{r['datetime']} - Open: {r['open']}, Close: {r['close']}, Volume: {r.get('volume', 'N/A')}")
        
        logger.info("Kiểm tra khoảng trống...")
        
        # Kiểm tra từ thời gian đầu đến cuối theo từng phút
        expected_time = start_time
        for actual_time in times:
            if actual_time != expected_time:
                # Tìm thấy khoảng trống
                gap_seconds = (actual_time - expected_time).total_seconds()
                gap_minutes = int(gap_seconds / 60)
                if gap_seconds > 60:  # Chỉ báo cáo khoảng trống > 1 phút
                    logger.warning(f"Khoảng trống: {expected_time} đến {actual_time} ({gap_minutes} phút)")
                    gap_count += 1
            
            # Cập nhật thời gian dự kiến tiếp theo
            expected_time = actual_time + timedelta(minutes=1)
            
        # Kiểm tra khoảng trống từ record cuối đến thời gian kết thúc
        if expected_time <= end_time:
            gap_seconds = (end_time - times[-1]).total_seconds()
            gap_minutes = int(gap_seconds / 60)
            if gap_seconds > 60:
                logger.warning(f"Khoảng trống cuối cùng: {times[-1]} đến {end_time} ({gap_minutes} phút)")
                gap_count += 1
        
        if gap_count > 0:
            logger.info(f"Tổng cộng có {gap_count} khoảng trống")
        else:
            logger.info("Không tìm thấy khoảng trống dữ liệu")
            
    except Exception as e:
        logger.exception(f"Lỗi khi kiểm tra khoảng trống: {e}")

def main():
    """Main function"""
    if len(sys.argv) < 3:
        print("Usage: python check_gaps.py <start_time> <end_time>")
        print("Example: python check_gaps.py '2025-10-13 07:00:00' '2025-10-13 08:00:00'")
        return 1
        
    start_time = sys.argv[1]
    end_time = sys.argv[2]
    
    check_data_gaps(start_time, end_time)
    return 0

if __name__ == "__main__":
    sys.exit(main())
