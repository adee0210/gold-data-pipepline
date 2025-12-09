# Gold Data Pipeline - Hệ thống thu thập dữ liệu vàng realtime

## Tổng quan

**Gold Data Pipeline** là hệ thống ETL (Extract, Transform, Load) thu thập, xử lý và lưu trữ dữ liệu giá vàng theo thời gian thực từ TradingView vào MongoDB. Hệ thống tự động phát hiện và điền khoảng trống dữ liệu, quản lý log thông minh với xoay vòng file log.

### Tính năng chính

- Thu thập dữ liệu realtime: Lấy dữ liệu giá vàng mỗi phút từ TradingView
- Tự động phát hiện khoảng trống: Tự động tìm và điền dữ liệu thiếu
- Quản lý số lượng bản ghi: Duy trì chính xác N bản ghi mới nhất
- Log rotation: Tự động quản lý file log khi quá lớn
- Điều khiển dịch vụ: Scripts quản lý start/stop/restart

## Kiến trúc hệ thống

```
┌─────────────────────────────────────────────┐
│           Gold Data Pipeline                │
├─────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐           │
│  │   Extract   │  │    Load     │           │
│  │             │  │             │           │
│  │ • TradingView│ │ • MongoDB   │           │
│  │             │  │ • Upsert    │           │
│  └─────────────┘  └─────────────┘           │
├─────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐           │
│  │   Config    │  │   Pipeline  │           │
│  │             │  │             │           │
│  │ • MongoDB   │  │ • Realtime  │           │
│  │ • TradingView│ │ • Backfill  │           │
│  │ • Logging   │  │ • Scheduler │           │
│  └─────────────┘  └─────────────┘           │
└─────────────────────────────────────────────┘
```

## Cấu trúc thư mục

```
gold-data-pipepline/
├── main.log                    # File log chính (tự động rotate)
├── requirements.txt            # Dependencies Python
├── run.sh                      # Script quản lý dịch vụ
├── config/                     # Cấu hình hệ thống
│   ├── logger_config.py        # Cấu hình logging với rotation
│   ├── mongo_config.py         # Kết nối MongoDB
│   ├── variable_config.py      # Biến cấu hình toàn cục
├── src/                        # Source code chính
│   ├── main.py                 # Entry point của ứng dụng
│   ├── etl/
│   │   ├── extract/
│   │   │   ├── realtime_metatrader_extract.py
│   │   │   └── historical_metatrader_extract.py
│   │   └── load/
│   │       ├── realtime_metatrader_load.py
│   │       └── historical_metatrader_load.py
│   ├── pipepline/
│   │   ├── realtime_metatrader_pipepline.py
│   │   └── historical_metatrader_pipepline.py
│   ├── utils/
│   │   └── tvdatafeed_adapter.py
├── test_backfill_data.py       # Script bù dữ liệu thiếu thủ công
```

## 🚀 Cài đặt và chạy

### 1. Chuẩn bị môi trường

```bash
git clone <repository-url>
cd gold-data-pipepline
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Cấu hình môi trường

Chỉnh sửa file `config/variable_config.py` để cấu hình MongoDB, TradingView, logging.

### 3. Khởi chạy hệ thống

```bash
./run.sh start    # Khởi động
./run.sh stop     # Dừng
./run.sh restart  # Khởi động lại
./run.sh status   # Kiểm tra trạng thái
```

### 4. Chạy thủ công (development)

```bash
python src/main.py
```

## ⚙️ Logic hoạt động

### 1. Pipeline Realtime
- Thu thập nến đã hoàn thành mỗi phút
- Upsert nến hiện tại mỗi 5 giây (n_bars=3)
- Tự động phát hiện và điền khoảng trống dữ liệu (24h gần nhất)
- Quản lý log với xoay vòng file (10MB, 5 bản backup)

### 2. Pipeline Historical
- Chỉ chạy khi database chưa có dữ liệu (import file lịch sử nếu cần)

### 3. Backfill thủ công
- Sử dụng script `test_backfill_data.py` để bù dữ liệu thiếu với số lượng lớn (ví dụ: 10000 bản ghi)

## 📊 Monitoring & Debugging

- Xem log realtime: `tail -f main.log`
- Kiểm tra trạng thái dịch vụ: `./run.sh status`
- Kiểm tra số lượng bản ghi: dùng MongoDB shell

## 🔒 Bảo mật
- Không commit thông tin nhạy cảm
- Đảm bảo database có authentication khi production

## 📝 Changelog

### v1.0.0
- ✅ Thu thập realtime từ TradingView
- ✅ Tự động phát hiện và điền khoảng trống
- ✅ Quản lý log rotation
- ✅ Script quản lý dịch vụ
- ✅ Script bù dữ liệu thiếu

---

**Gold Data Pipeline** - Hệ thống thu thập dữ liệu vàng realtime, tự động, bảo trì dễ dàng.
