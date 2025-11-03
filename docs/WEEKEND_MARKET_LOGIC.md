# Weekend Market Closed Logic

## Tổng quan

Hệ thống đã được cập nhật để **tự động nhận biết** khi thị trường vàng đóng cửa vào **Thứ 7 và Chủ nhật**, và sẽ **KHÔNG gửi cảnh báo** thiếu data trong những thời điểm này.

## 🕐 Lịch thị trường vàng

### Thị trường MỞ CỬA
- **Thứ 2 - Thứ 6**: Cả ngày
- **Thứ 7**: 00:00 - 06:00 (có thể có data)

### Thị trường ĐÓNG CỬA  
- **Thứ 7**: Sau 06:00 sáng
- **Chủ nhật**: Cả ngày

## ✅ Hành vi hệ thống

### 1. Alert "No Data From Source"
```python
# Ngày thường (T2-T6)
alert_no_data_from_source()  
→ ✅ GỬI CẢNH BÁO

# T7/CN (thị trường đóng)
alert_no_data_from_source()  
→ ❌ KHÔNG GỬI (log: "Bỏ qua - Thị trường đóng cửa")
```

### 2. Alert "No New Data"
```python
# Ngày thường
alert_no_new_data()  
→ ✅ GỬI CẢNH BÁO nếu >1 phút không có data

# T7/CN
alert_no_new_data()  
→ ❌ KHÔNG GỬI (log: "Bỏ qua - Thị trường đóng cửa")
```

### 3. Alert "Gap Detected"

**Khoảng trống HOÀN TOÀN trong T7/CN:**
```python
# Gap từ T7 8h đến CN 20h
alert_gap_detected(sat_8am, sun_8pm, 720)  
→ ❌ KHÔNG GỬI (log: "Bỏ qua - Khoảng trống trong T7/CN")
```

**Khoảng trống BẮT ĐẦU từ ngày thường:**
```python
# Gap từ T6 23h đến T2 8h (qua T7/CN)
alert_gap_detected(fri_11pm, mon_8am, 2940)  
→ ✅ VẪN GỬI CẢNH BÁO (gap không hoàn toàn trong T7/CN)
```

### 4. Tự động lọc Gap khi Check & Fix

```python
check_and_fix_gaps(lookback_hours=24)
→ Tự động BỎ QUA các gap nằm trong T7/CN
→ CHỈ xử lý gaps ngoài thời gian thị trường đóng
```

## 📋 Logic Implementation

### Kiểm tra thị trường đóng cửa

```python
def _is_market_closed_time(dt: datetime) -> bool:
    weekday = dt.weekday()
    
    # Chủ nhật (6): Đóng cả ngày
    if weekday == 6:
        return True
    
    # Thứ 7 (5): Đóng sau 6h sáng
    if weekday == 5 and dt.hour >= 6:
        return True
    
    return False
```

## 🧪 Test

```bash
# Test logic weekend
python test_weekend_logic.py

# Sẽ kiểm tra:
# - Detection thứ 7, chủ nhật
# - Alert behavior trong các trường hợp
# - Gap filtering logic
```

## 📊 Ví dụ Log

### Ngày thường (T2-T6):
```
2025-11-03 14:18:09 - WARNING - No data returned from TV adapter
2025-11-03 14:18:09 - INFO - Đã gửi cảnh báo Discord: no_data_TradingView
```

### Thứ 7/Chủ nhật:
```
2025-11-08 10:00:00 - WARNING - No data returned from TV adapter
2025-11-08 10:00:00 - INFO - Bỏ qua cảnh báo no_data từ TradingView - Thị trường đóng cửa (T7/CN)
```

## ⚙️ Cấu hình

Thời gian đóng cửa T7 có thể điều chỉnh trong `discord_alert_util.py`:

```python
def _is_market_closed_time(self, dt: datetime) -> bool:
    # ...
    if weekday == 5 and dt.hour >= 6:  # Thay đổi số 6 nếu cần
        return True
```

## 🎯 Kết quả

✅ **Giảm false alerts** vào cuối tuần  
✅ **Log rõ ràng** lý do bỏ qua alert  
✅ **Vẫn detect** lỗi thực sự (network errors, etc.)  
✅ **Không cố fill** data cho T7/CN  
✅ **Smart gap detection** tự động lọc T7/CN

## 📝 Notes

- Thời gian dựa trên local timezone của server
- Logic có thể điều chỉnh theo múi giờ giao dịch
- Vẫn gửi alert cho các lỗi THỰC SỰ (connection errors, format errors, etc.)
- Chỉ bỏ qua alerts liên quan đến "no data" vào T7/CN
