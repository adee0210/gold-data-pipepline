#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sửa lại realtime pipeline để kiểm tra và tự động sửa dữ liệu thiếu
"""

import sys
import os
import time
from datetime import datetime, timedelta

# Thêm thư mục gốc vào sys.path để import các module trong project
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from config.logger_config import LoggerConfig
from src.pipepline.realtime_metatrader_pipepline import RealtimeMetatraderPipepline

# Cấu hình logger
logger = LoggerConfig.logger_config("Enhanced Realtime Pipeline")

class EnhancedRealtimeMetatraderPipeline(RealtimeMetatraderPipepline):
    """
    Cải tiến pipeline realtime với chức năng kiểm tra và sửa dữ liệu thiếu tự động
    """
    
    def __init__(self):
        super().__init__()
        # Thời điểm kiểm tra dữ liệu thiếu cuối cùng
        self.last_gap_check = datetime.now()
        # Khoảng thời gian kiểm tra: mỗi 15 phút
        self.gap_check_interval = timedelta(minutes=15)
        
    def check_and_fix_recent_gaps(self):
        """
        Kiểm tra và sửa dữ liệu thiếu trong 24 giờ qua
        """
        now = datetime.now()
        
        # Chỉ kiểm tra nếu đã đủ thời gian từ lần kiểm tra cuối
        if now - self.last_gap_check < self.gap_check_interval:
            return
            
        logger.info("Bắt đầu kiểm tra dữ liệu thiếu trong 24 giờ qua")
        self.last_gap_check = now
        
        # Thời gian bắt đầu kiểm tra: 24 giờ trước
        start_time = now - timedelta(hours=24)
        start_time = start_time.replace(second=0, microsecond=0)
        
        # Thời gian kết thúc kiểm tra: thời điểm hiện tại
        end_time = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        
        logger.info(f"Kiểm tra dữ liệu từ {start_time} đến {end_time}")
        
        # Lấy các records trong khoảng thời gian
        collection = self.extractor.gold_collection
        records = list(collection.find({
            "datetime": {"$gte": start_time, "$lte": end_time}
        }).sort("datetime", 1))
        
        if not records:
            logger.warning("Không có dữ liệu trong 24 giờ qua!")
            # TODO: Thực hiện lấy lại toàn bộ dữ liệu 24h qua
            return
            
        # Tìm các khoảng thiếu
        missing_ranges = []
        current_time = start_time
        
        for record in records:
            record_time = record["datetime"]
            
            # Nếu có khoảng trống > 1 phút
            if record_time > current_time + timedelta(minutes=1):
                missing_ranges.append((current_time, record_time - timedelta(minutes=1)))
                
            # Cập nhật current_time
            current_time = record_time + timedelta(minutes=1)
            
        # Kiểm tra khoảng trống cuối cùng
        if end_time > records[-1]["datetime"] + timedelta(minutes=1):
            missing_ranges.append((records[-1]["datetime"] + timedelta(minutes=1), end_time))
            
        if not missing_ranges:
            logger.info("Không tìm thấy khoảng trống dữ liệu trong 24 giờ qua")
            return
            
        # In thông tin các khoảng trống
        total_missing_minutes = sum((end - start).total_seconds() // 60 + 1 for start, end in missing_ranges)
        logger.info(f"Tìm thấy {len(missing_ranges)} khoảng trống với tổng cộng {int(total_missing_minutes)} phút dữ liệu bị thiếu")
        
        # Sửa từng khoảng trống
        for start_gap, end_gap in missing_ranges:
            logger.info(f"Đang lấy dữ liệu cho khoảng trống: {start_gap} đến {end_gap}")
            
            # Lấy dữ liệu từ TradingView
            df = self.extractor.fetch_historical_range(start_gap, end_gap)
            
            if df is not None and not df.empty:
                # Cập nhật vào database
                self.loader.realtime_load(df)
                logger.info(f"Đã cập nhật {len(df)} records vào database")
            else:
                logger.warning(f"Không lấy được dữ liệu cho khoảng trống {start_gap} đến {end_gap}")
                
    def run_realtime(self):
        """
        Chạy pipeline realtime với cải tiến kiểm tra khoảng trống
        """
        import schedule
        
        # Giữ lại các schedule từ lớp cha
        schedule.every(1).minutes.do(self.run_once)
        schedule.every(5).seconds.do(self.upsert_current_minute)
        
        # Thêm schedule kiểm tra khoảng trống
        schedule.every(15).minutes.do(self.check_and_fix_recent_gaps)
        
        logger.info("Realtime pipeline cải tiến đã khởi động:")
        logger.info("- Mỗi 1 phút: Lấy các nến thiếu (ưu tiên cao)")
        logger.info("- Mỗi 5 giây: Cập nhật nến hiện tại")
        logger.info("- Mỗi 15 phút: Kiểm tra và sửa khoảng trống dữ liệu trong 24 giờ qua")
        logger.info("Nhấn Ctrl+C để dừng.")
        
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    # Thêm method cần thiết vào RealtimeMetatraderExtract
    from src.etl.extract.realtime_metatrader_extract import RealtimeMetatraderExtract
    
    def fetch_historical_range(self, start_time, end_time):
        """
        Lấy dữ liệu lịch sử trong khoảng thời gian cụ thể
        
        Args:
            start_time (datetime): Thời điểm bắt đầu
            end_time (datetime): Thời điểm kết thúc
            
        Returns:
            DataFrame: DataFrame chứa dữ liệu trong khoảng thời gian
        """
        import pandas as pd
        from tvDatafeed import TvDatafeed, Interval
        
        self.logger.info(f"Lấy dữ liệu lịch sử từ {start_time} đến {end_time}")
        
        # Tính số phút trong khoảng
        time_range_minutes = int((end_time - start_time).total_seconds() // 60) + 1
        
        if time_range_minutes > 5000:
            self.logger.warning(f"Khoảng thời gian quá lớn: {time_range_minutes} phút > 5000 phút")
            return None
        
        try:
            # Khởi tạo TvDatafeed
            tv = TvDatafeed()
            
            # Lấy dữ liệu
            df = tv.get_hist(
                symbol=self.symbol, 
                exchange=self.exchange,
                interval=Interval.in_1_minute,
                n_bars=time_range_minutes
            )
            
            if df is None or df.empty:
                self.logger.warning("Không lấy được dữ liệu từ TradingView")
                return None
                
            # Format dữ liệu
            df = df.reset_index()
            
            # Tạo trường datetime
            df["datetime"] = pd.to_datetime(df.index)
            
            # Filter dữ liệu trong khoảng
            df = df[(df["datetime"] >= start_time) & (df["datetime"] <= end_time)]
            
            if df.empty:
                self.logger.warning("Sau khi filter, không còn dữ liệu")
                return None
                
            # Đổi tên vol thành volume
            df = df.rename(columns={"volume": "volume"})
            
            self.logger.info(f"Đã lấy được {len(df)} records từ TradingView")
            return df
            
        except Exception as e:
            self.logger.exception(f"Lỗi khi lấy dữ liệu lịch sử: {e}")
            return None
    
    # Thêm method vào class
    RealtimeMetatraderExtract.fetch_historical_range = fetch_historical_range
    
    # Khởi động pipeline cải tiến
    pipeline = EnhancedRealtimeMetatraderPipeline()
    pipeline.run_realtime()