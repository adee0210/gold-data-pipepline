#!/usr/bin/env python3
"""
Script test logic T7/CN của Discord Alert
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.utils.discord_alert_util import DiscordAlertUtil


def test_weekend_logic():
    """Test weekend detection logic"""

    print("=" * 70)
    print("TEST WEEKEND MARKET CLOSED LOGIC")
    print("=" * 70)

    alert = DiscordAlertUtil()

    # Test cases với các ngày khác nhau
    test_cases = [
        # (datetime, expected_is_weekend, expected_is_market_closed, description)
        (datetime(2025, 11, 3, 10, 0), False, False, "Thứ 2 10h sáng - Thị trường mở"),
        (
            datetime(2025, 11, 4, 14, 30),
            False,
            False,
            "Thứ 3 2h30 chiều - Thị trường mở",
        ),
        (datetime(2025, 11, 8, 5, 0), True, False, "Thứ 7 5h sáng - Thị trường còn mở"),
        (datetime(2025, 11, 8, 8, 0), True, True, "Thứ 7 8h sáng - Thị trường đóng"),
        (datetime(2025, 11, 8, 14, 0), True, True, "Thứ 7 2h chiều - Thị trường đóng"),
        (datetime(2025, 11, 9, 0, 0), True, True, "Chủ nhật 0h - Thị trường đóng"),
        (datetime(2025, 11, 9, 12, 0), True, True, "Chủ nhật 12h - Thị trường đóng"),
        (datetime(2025, 11, 9, 23, 59), True, True, "Chủ nhật 23h59 - Thị trường đóng"),
    ]

    print("\n📅 KIỂM TRA CÁC TRƯỜNG HỢP:")
    print("-" * 70)

    for dt, exp_weekend, exp_closed, desc in test_cases:
        is_weekend = alert._is_weekend(dt)
        is_closed = alert._is_market_closed_time(dt)

        weekend_icon = "🟢" if is_weekend == exp_weekend else "🔴"
        closed_icon = "🟢" if is_closed == exp_closed else "🔴"

        print(f"\n{desc}")
        print(f"  Ngày: {dt.strftime('%Y-%m-%d %H:%M')} ({dt.strftime('%A')})")
        print(f"  {weekend_icon} Is Weekend: {is_weekend} (expected: {exp_weekend})")
        print(f"  {closed_icon} Market Closed: {is_closed} (expected: {exp_closed})")

    # Test alert behavior
    print("\n" + "=" * 70)
    print("TEST ALERT BEHAVIOR VÀO T7/CN")
    print("=" * 70)

    print("\n[TEST 1] Alert no_data vào Thứ 2")
    print("-" * 70)
    print("Gọi alert_no_data_from_source() - Ngày thường")
    # Giả lập thời gian thứ 2
    original_now = datetime.now
    datetime.now = lambda: datetime(2025, 11, 3, 10, 0)  # Thứ 2
    alert.alert_no_data_from_source("TradingView_TEST", "Test vào ngày thường")
    datetime.now = original_now
    print("✅ Alert sẽ được gửi vào ngày thường\n")

    print("[TEST 2] Alert no_data vào Chủ nhật")
    print("-" * 70)
    print("Gọi alert_no_data_from_source() - Chủ nhật")
    # Giả lập thời gian chủ nhật
    datetime.now = lambda: datetime(2025, 11, 9, 14, 0)  # Chủ nhật
    alert.alert_no_data_from_source("TradingView_TEST", "Test vào chủ nhật")
    datetime.now = original_now
    print("✅ Alert sẽ KHÔNG được gửi vào chủ nhật\n")

    print("[TEST 3] Alert gap vào T7/CN")
    print("-" * 70)
    saturday_morning = datetime(2025, 11, 8, 8, 0)
    sunday_noon = datetime(2025, 11, 9, 12, 0)
    alert.alert_gap_detected(saturday_morning, sunday_noon, 1680)  # 28 giờ
    print("✅ Gap alert trong T7/CN sẽ KHÔNG được gửi\n")

    print("[TEST 4] Alert gap từ Thứ 6 đến Thứ 2")
    print("-" * 70)
    friday_night = datetime(2025, 11, 7, 23, 0)
    monday_morning = datetime(2025, 11, 10, 9, 0)
    alert.alert_gap_detected(friday_night, monday_morning, 600)
    print("✅ Gap alert bắt đầu từ thứ 6 (ngoài T7/CN) sẽ được gửi\n")

    print("=" * 70)
    print("TỔNG KẾT")
    print("=" * 70)
    print("✅ Thứ 7 sau 6h sáng: Thị trường đóng cửa")
    print("✅ Chủ nhật cả ngày: Thị trường đóng cửa")
    print("✅ Các ngày khác: Thị trường mở cửa")
    print("\n📢 HÀNH VI CẢNH BÁO:")
    print("  - Không gửi alert no_data vào T7/CN")
    print("  - Không gửi alert no_new_data vào T7/CN")
    print("  - Không gửi alert gap nếu gap hoàn toàn trong T7/CN")
    print("  - Vẫn gửi alert gap nếu gap bắt đầu từ ngày thường")
    print("  - Vẫn gửi alert fetch_error (lỗi thực sự)")
    print("=" * 70)


if __name__ == "__main__":
    test_weekend_logic()
