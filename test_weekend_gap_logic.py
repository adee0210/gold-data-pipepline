#!/usr/bin/env python3
"""
Script test logic weekend gap detection
"""
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from src.utils.discord_alert_util import DiscordAlertUtil


def test_weekend_gap_logic():
    """Test các trường hợp gap detection với weekend"""

    alert = DiscordAlertUtil()

    test_cases = [
        # (start_time, end_time, description, should_skip)
        (
            datetime(2025, 11, 2, 15, 43, 0),  # Chủ nhật 15:43
            datetime(2025, 11, 3, 6, 3, 0),  # Thứ Hai 06:03
            "Gap cuối tuần: CN 15:43 → T2 06:03",
            True,
        ),
        (
            datetime(2025, 11, 4, 5, 44, 0),  # Thứ Hai 05:44
            datetime(2025, 11, 4, 6, 3, 0),  # Thứ Hai 06:03
            "Gap ngoài giờ: T2 05:44 → T2 06:03 (trước 6h sáng)",
            True,  # Skip - ngoài giờ giao dịch
        ),
        (
            datetime(2025, 11, 1, 10, 0, 0),  # Thứ 7 10:00
            datetime(2025, 11, 1, 12, 0, 0),  # Thứ 7 12:00
            "Gap trong thứ 7: T7 10:00 → T7 12:00",
            True,  # Vì bắt đầu sau 6h sáng T7
        ),
        (
            datetime(2025, 11, 3, 10, 0, 0),  # Thứ Hai 10:00
            datetime(2025, 11, 3, 12, 0, 0),  # Thứ Hai 12:00
            "Gap trong giờ giao dịch: T2 10:00 → T2 12:00",
            False,  # Không skip - gap trong tuần
        ),
        (
            datetime(2025, 11, 2, 10, 0, 0),  # Chủ nhật 10:00
            datetime(2025, 11, 2, 12, 0, 0),  # Chủ nhật 12:00
            "Gap toàn bộ CN: CN 10:00 → CN 12:00",
            True,  # Skip - CN toàn bộ
        ),
        (
            datetime(2025, 11, 1, 3, 0, 0),  # Thứ 7 03:00
            datetime(2025, 11, 1, 5, 0, 0),  # Thứ 7 05:00
            "Gap đầu T7 (trước 6h): T7 03:00 → T7 05:00",
            True,  # Skip - ngoài giờ giao dịch (trước 6h)
        ),
        (
            datetime(2025, 11, 4, 3, 0, 0),  # Thứ Hai 03:00
            datetime(2025, 11, 4, 5, 30, 0),  # Thứ Hai 05:30
            "Gap rạng sáng T2: T2 03:00 → T2 05:30",
            True,  # Skip - ngoài giờ giao dịch (trước 6h)
        ),
    ]

    print("=" * 80)
    print("TEST WEEKEND GAP DETECTION LOGIC")
    print("=" * 80)
    print()

    for start_time, end_time, description, should_skip in test_cases:
        gap_minutes = int((end_time - start_time).total_seconds() // 60)

        print(f"Test: {description}")
        print(f"  Start: {start_time.strftime('%Y-%m-%d (%A) %H:%M')}")
        print(f"  End:   {end_time.strftime('%Y-%m-%d (%A) %H:%M')}")
        print(f"  Gap:   {gap_minutes} phút")

        # Check logic
        start_closed = alert._is_market_closed_time(start_time)
        end_closed = alert._is_market_closed_time(end_time)
        will_skip = start_closed  # New logic: skip if START is in closed time

        print(f"  Start closed: {start_closed}")
        print(f"  End closed:   {end_closed}")
        print(f"  Will skip:    {will_skip}")
        print(f"  Expected:     {should_skip}")

        if will_skip == should_skip:
            print(f"  ✅ PASS")
        else:
            print(f"  ❌ FAIL - Logic không đúng!")

        print()

    print("=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    test_weekend_gap_logic()
