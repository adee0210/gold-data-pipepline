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


### 1. Chuẩn bị môi trường

```bash
git clone <repository-url>
cd gold-data-pipepline
# Môi trường ảo sẽ được tạo tự động khi chạy main.py lần đầu
```

### 2. Cấu hình môi trường

Tạo file `.env` trong thư mục gốc của project và cấu hình các biến môi trường sau:

```bash
# MongoDB Configuration
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_USER=your_mongo_username
MONGO_PASS=your_mongo_password
MONGO_AUTH=admin

# Database Configuration
GOLD_DB_NAME=gold_db
GOLD_COLLECTION_NAME=gold_minute_data
BATCH_SIZE=10000

# TradingView Configuration
TV_MAX_RETRIES=3
TV_RETRY_DELAY=5
TV_DEFAULT_N_BARS=100

# Historical Data Source
GDRIVE_URL=https://drive.google.com/uc?id=1v7HVgXhUmGEUbmbkPxpZ44RiUJH8V3NK
```

**Lưu ý:** 
- Các biến có giá trị mặc định có thể bỏ qua
- Không commit file `.env` vào git (đã được ignore)

### 3. Khởi chạy hệ thống

```bash
python src/main.py start    # Khởi động
python src/main.py stop     # Dừng
python src/main.py restart  # Khởi động lại
python src/main.py status   # Kiểm tra trạng thái
python src/main.py monitor  # Chạy mode monitor (auto-restart nếu crash)
```

### 4. Chạy thủ công (development)

```bash
python src/main.py monitor  # Chạy liên tục với auto-restart
```

## Logic hoạt động

### 1. Pipeline Realtime
- Upsert 10 nến gần nhất mỗi 10 giây
- Quản lý log với xoay vòng file (10MB, 5 bản backup)

### 2. Pipeline Historical
- Chạy khi database chưa có dữ liệu, import 5000 nến lịch sử

### 3. Backfill thủ công
- Sử dụng script `test_backfill_data.py` để bù dữ liệu thiếu với số lượng lớn (ví dụ: 10000 bản ghi)

## Monitoring & Debugging

- Xem log realtime: `tail -f main.log`
- Kiểm tra trạng thái dịch vụ: `python src/main.py status`
- Kiểm tra số lượng bản ghi: dùng MongoDB shell