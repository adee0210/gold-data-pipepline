# Gold Data Pipeline - Hệ thống thu thập dữ liệu vàng realtime

## 📋 Tổng quan

**Gold Data Pipeline** là một hệ thống ETL (Extract, Transform, Load) hoàn chỉnh được thiết kế để thu thập, xử lý và lưu trữ dữ liệu giá vàng theo thời gian thực từ TradingView. Hệ thống sử dụng kiến trúc pipeline hiện đại với khả năng tự động phát hiện và sửa chữa khoảng trống dữ liệu, tích hợp cảnh báo Discord, và quản lý log thông minh.

### ✨ Tính năng chính

- 🔄 **Thu thập dữ liệu realtime**: Lấy dữ liệu giá vàng mỗi phút từ TradingView
- 📊 **Xử lý dữ liệu lịch sử**: Import dữ liệu lịch sử từ Google Drive
- 🔍 **Tự động phát hiện khoảng trống**: Tự động tìm và điền dữ liệu thiếu
- 📏 **Quản lý số lượng bản ghi**: Duy trì chính xác N bản ghi mới nhất
- 🚨 **Cảnh báo Discord**: Thông báo lỗi realtime qua Discord webhook
- 📝 **Log rotation**: Tự động quản lý file log khi quá lớn
- 🛠️ **API REST**: Cung cấp API để truy vấn dữ liệu
- 🎛️ **Điều khiển dịch vụ**: Scripts quản lý start/stop/restart

## 🏗️ Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────┐
│                    Gold Data Pipeline                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Extract   │  │  Transform  │  │    Load     │         │
│  │             │  │             │  │             │         │
│  │ • TradingView│  │ • Validate  │  │ • MongoDB   │         │
│  │ • Google     │  │ • Clean     │  │ • Upsert    │         │
│  │   Drive      │  │ • Format    │  │ • Batch     │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Config    │  │   Utils     │  │   Pipeline  │         │
│  │             │  │             │  │             │         │
│  │ • MongoDB   │  │ • Discord   │  │ • Realtime  │         │
│  │ • TradingView│  │ • TVAdapter│  │ • Historical│         │
│  │ • Discord    │  │ • Logger   │  │ • Scheduler │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

## 📁 Cấu trúc thư mục

```
gold-data-pipepline/
├── 📄 main.log                    # File log chính (tự động rotate)
├── 📄 requirements.txt            # Dependencies Python
├── 📄 run.sh                      # Script quản lý dịch vụ
├── 📄 .env.example               # Template cấu hình môi trường
├── 📁 config/                    # Cấu hình hệ thống
│   ├── 📄 logger_config.py       # Cấu hình logging với rotation
│   ├── 📄 mongo_config.py        # Kết nối MongoDB
│   ├── 📄 variable_config.py     # Biến cấu hình toàn cục
│   └── 📄 advanced_logger_config.py # Logger nâng cao với nén
├── 📁 src/                       # Source code chính
│   ├── 📄 main.py                # Entry point của ứng dụng
│   ├── 📁 etl/                   # Extract, Transform, Load
│   │   ├── 📁 extract/           # Logic trích xuất dữ liệu
│   │   │   ├── 📄 realtime_metatrader_extract.py
│   │   │   └── 📄 historical_metatrader_extract.py
│   │   └── 📁 load/              # Logic tải dữ liệu
│   │       ├── 📄 realtime_metatrader_load.py
│   │       └── 📄 historical_metatrader_load.py
│   ├── 📁 pipepline/             # Orchestration pipelines
│   │   ├── 📄 realtime_metatrader_pipepline.py
│   │   └── 📄 historical_metatrader_pipepline.py
│   └── 📁 utils/                 # Utilities và helpers
│       ├── 📄 discord_alert_util.py     # Cảnh báo Discord
│       └── 📄 tvdatafeed_adapter.py     # Adapter TradingView
├── 📁 docs/                      # Documentation
│   ├── 📄 DISCORD_ALERTS.md      # Hướng dẫn Discord alerts
│   ├── 📄 DISCORD_FLOW.md        # Flow diagram Discord
│   └── 📄 LOG_ROTATION.md        # Hướng dẫn log rotation
├── 📁 scripts/                   # Scripts tiện ích
│   ├── 📄 test_discord_quick.py  # Test Discord nhanh
│   ├── 📄 test_discord_alert.py  # Test Discord đầy đủ
│   ├── 📄 check_log_status.py    # Kiểm tra trạng thái log
│   └── 📄 fill_data_gaps.py      # Script điền khoảng trống
└── 📁 data/                      # Thư mục dữ liệu (tạo tự động)
```

## 🚀 Cài đặt và chạy

### 1. Chuẩn bị môi trường

```bash
# Clone repository
git clone <repository-url>
cd gold-data-pipepline

# Tạo virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# hoặc .venv\Scripts\activate  # Windows

# Cài đặt dependencies
pip install -r requirements.txt
```

### 2. Cấu hình môi trường

```bash
# Copy file cấu hình mẫu
cp .env.example .env

# Chỉnh sửa file .env theo nhu cầu
nano .env
```

**Nội dung file .env:**
```env
# MongoDB Configuration
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_USER=your_username
MONGO_PASS=your_password
MONGO_AUTH=admin

# TradingView Configuration (tùy chọn)
TV_SYMBOL=XAUUSD
TV_EXCHANGE=OANDA

# Discord Alerts (tùy chọn nhưng khuyến nghị)
DISCORD_ALERT_ENABLED=true
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN
```

### 3. Khởi chạy hệ thống

```bash
# Chạy script quản lý dịch vụ
./run.sh start    # Khởi động
./run.sh stop     # Dừng
./run.sh restart  # Khởi động lại
./run.sh status   # Kiểm tra trạng thái
```

### 4. Chạy thủ công (development)

```bash
# Chạy trực tiếp
python src/main.py

# Hoặc chạy với các tùy chọn
python src/pipepline/realtime_metatrader_pipepline.py --maintain-latest --n-bars 5000
```

## ⚙️ Cấu hình chi tiết

### MongoDB Configuration

```python
MONGO_CONFIG = {
    "host": "localhost",           # Địa chỉ MongoDB server
    "port": 27017,                 # Port MongoDB
    "user": "your_username",       # Username (nếu có)
    "pass": "your_password",       # Password (nếu có)
    "authSource": "admin"          # Database xác thực
}
```

### Gold Data Configuration

```python
GOLD_DATA_CONFIG = {
    "database": "gold_db",                           # Tên database
    "collection": "gold_minute_data",                # Tên collection
    "batch_size_extract": 10000,                     # Kích thước batch khi extract
    "metatrader_data_gdrive_url": "...",             # URL Google Drive chứa data lịch sử
    "metatrader_data_local_path": "data/gold_data_metatrader5.csv"
}
```

### Discord Alerts Configuration

```python
DISCORD_CONFIG = {
    "webhook_url": "https://discord.com/api/webhooks/...",  # Discord webhook URL
    "enabled": True                                          # Bật/tắt cảnh báo
}
```

## 🔧 Chức năng chính

### 1. Pipeline Realtime (RealtimeMetatraderPipepline)

**Mục đích**: Thu thập dữ liệu giá vàng theo thời gian thực từ TradingView

**Chức năng chính**:
- **Thu thập dữ liệu mỗi phút**: Lấy các nến đã hoàn thành
- **Cập nhật nến hiện tại**: Upsert nến đang hình thành mỗi 5 giây
- **Tự động sửa khoảng trống**: Phát hiện và điền dữ liệu thiếu
- **Duy trì số lượng bản ghi**: Giữ chính xác N bản ghi mới nhất

**Lịch trình hoạt động**:
- **Mỗi 1 phút**: Thu thập nến đã hoàn thành
- **Mỗi 5 giây**: Cập nhật nến hiện tại (khi data up-to-date)
- **Mỗi 4 giờ**: Kiểm tra và sửa khoảng trống lịch sử

### 2. Pipeline Historical (HistoricalMetatraderPipepline)

**Mục đích**: Import dữ liệu lịch sử từ Google Drive

**Chức năng chính**:
- **Tải dữ liệu từ Google Drive**: Sử dụng gdown để download
- **Xử lý định dạng**: Chuyển đổi format từ MetaTrader 5
- **Import vào MongoDB**: Batch insert với xử lý lỗi

**Quy trình**:
1. Download file CSV từ Google Drive
2. Parse và clean dữ liệu
3. Transform sang format chuẩn
4. Load vào MongoDB với batch processing

### 3. Extract Layer

#### RealtimeMetatraderExtract
- **fetch_realtime_data()**: Lấy data realtime từ TradingView
- **get_current_minute_candle()**: Lấy nến phút hiện tại
- **check_and_fix_gaps()**: Phát hiện và sửa khoảng trống
- **maintain_latest_n_bars()**: Duy trì N bản ghi mới nhất
- **fetch_historical_range()**: Lấy data lịch sử trong khoảng thời gian

#### HistoricalMetatraderExtract
- **historical_extract()**: Extract từ Google Drive CSV

### 4. Load Layer

#### RealtimeMetatraderLoad
- **realtime_load()**: Batch insert/update dữ liệu realtime
- **upsert_current_minute_candle()**: Upsert nến hiện tại
- **Xử lý lỗi**: Bulk write errors, duplicate handling

#### HistoricalMetatraderLoad
- **historical_load()**: Import dữ liệu lịch sử

### 5. Utilities

#### DiscordAlertUtil
**Mục đích**: Gửi cảnh báo lỗi qua Discord

**Các loại cảnh báo**:
- 🚨 **alert_no_data_from_source**: Không có data từ nguồn
- ❌ **alert_data_fetch_error**: Lỗi khi fetch data
- ⚠️ **alert_data_format_error**: Lỗi định dạng data
- ⏱️ **alert_no_new_data**: Không có data mới (>1 phút)
- 📊 **alert_gap_detected**: Phát hiện khoảng trống
- 💾 **alert_database_error**: Lỗi database

**Tính năng chống spam**:
- Cooldown 5 phút giữa các cảnh báo cùng loại
- Chỉ cảnh báo gap > 5 phút
- Chỉ cảnh báo khi thực sự có lỗi

#### TVDataFeedAdapter
**Mục đích**: Adapter để giao tiếp với TradingView API

**Chức năng**:
- Chuẩn hóa format dữ liệu từ tvDatafeed
- Xử lý lỗi và retry logic
- Transform data sang format nội bộ

## 📊 Cơ chế hoạt động

### 1. Khởi động hệ thống

```
1. Chạy Historical Pipeline
   ├── Download data từ Google Drive
   ├── Process và clean data
   └── Import vào MongoDB

2. Khởi động Realtime Pipeline
   ├── Kiểm tra khoảng trống lịch sử
   ├── Setup scheduler
   └── Bắt đầu thu thập realtime
```

### 2. Vòng lặp realtime

```
Mỗi phút:
├── Thu thập nến đã hoàn thành
├── Kiểm tra khoảng trống
└── Cập nhật database

Mỗi 5 giây:
├── Cập nhật nến hiện tại (nếu data up-to-date)
└── Kiểm tra trạng thái data

Mỗi 4 giờ:
├── Kiểm tra khoảng trống 24h qua
├── Điền dữ liệu thiếu
└── Gửi cảnh báo nếu cần
```

### 3. Log Rotation

```
Khi main.log đạt 50MB:
├── main.log → main.log.1
├── main.log.1 → main.log.2
├── main.log.2 → main.log.3
├── main.log.3 → main.log.4
├── main.log.4 → main.log.5
├── main.log.5 → XÓA
└── Tạo main.log mới
```

## 🔍 Monitoring và Debugging

### Kiểm tra trạng thái hệ thống

```bash
# Kiểm tra trạng thái dịch vụ
./run.sh status

# Kiểm tra log files
python scripts/check_log_status.py

# Test Discord alerts
python scripts/test_discord_quick.py
```

### Log Analysis

```bash
# Xem log realtime
tail -f main.log

# Tìm lỗi trong log
grep "ERROR" main.log

# Đếm số lượng cảnh báo Discord
grep "Đã gửi cảnh báo Discord" main.log | wc -l
```

### Database Queries

```bash
# Kết nối MongoDB
mongosh gold_db

# Đếm tổng số bản ghi
db.gold_minute_data.countDocuments()

# Tìm bản ghi mới nhất
db.gold_minute_data.find().sort({datetime: -1}).limit(1)

# Kiểm tra khoảng trống dữ liệu
db.gold_minute_data.aggregate([
  {$group: {_id: null, min: {$min: "$datetime"}, max: {$max: "$datetime"}}}
])
```

## 🛠️ Scripts tiện ích

### Quản lý dịch vụ
```bash
./run.sh start    # Khởi động
./run.sh stop     # Dừng
./run.sh restart  # Khởi động lại
./run.sh status   # Trạng thái
```

### Test và Debug
```bash
# Test Discord webhook
python scripts/test_discord_quick.py

# Test đầy đủ Discord alerts
python scripts/test_discord_alert.py

# Kiểm tra trạng thái log
python scripts/check_log_status.py

# Điền khoảng trống data
python scripts/fill_data_gaps.py --start_date 2025-10-09 --end_date 2025-10-13
```

## 📋 API Endpoints (tương lai)

Hệ thống có thể mở rộng để cung cấp REST API:

```
GET /api/gold/latest           # Lấy nến mới nhất
GET /api/gold/range            # Lấy data trong khoảng thời gian
GET /api/gold/stats            # Thống kê database
GET /api/health                # Health check
```

## 🔒 Bảo mật

### Environment Variables
- File `.env` đã được thêm vào `.gitignore`
- Không commit thông tin nhạy cảm (password, webhook URL)

### Database Security
- Sử dụng authentication khi production
- Limit network access
- Regular backup

### Discord Webhook
- Sử dụng webhook riêng cho alerts
- Không share webhook URL công khai
- Thay đổi webhook khi nghi ngờ bị lộ

## 🚨 Troubleshooting

### Lỗi thường gặp

#### 1. Không kết nối được MongoDB
```
Lỗi: "Cannot connect to MongoDB"
Giải pháp:
- Kiểm tra MongoDB đang chạy: sudo systemctl status mongod
- Kiểm tra cấu hình trong .env
- Kiểm tra firewall
```

#### 2. Không có data từ TradingView
```
Lỗi: "No data returned from TV adapter"
Giải pháp:
- Kiểm tra internet connection
- TradingView có thể rate limit
- Thử restart pipeline
```

#### 3. Discord alerts không hoạt động
```
Lỗi: "Discord alerts TẮT"
Giải pháp:
- Kiểm tra DISCORD_ALERT_ENABLED=true trong .env
- Kiểm tra DISCORD_WEBHOOK_URL đúng format
- Test webhook: python scripts/test_discord_quick.py
```

#### 4. Log file quá lớn
```
Vấn đề: main.log > 50MB nhưng không rotate
Giải pháp:
- Restart pipeline để áp dụng cấu hình mới
- Kiểm tra quyền ghi file
```

### Debug Steps

```bash
# 1. Kiểm tra logs
tail -f main.log

# 2. Test từng component
python -c "from src.etl.extract.realtime_metatrader_extract import RealtimeMetatraderExtract; e = RealtimeMetatraderExtract(); print('Extract OK')"

# 3. Test database connection
python -c "from config.mongo_config import MongoConfig; c = MongoConfig(); print('MongoDB OK')"

# 4. Test Discord webhook
python scripts/test_discord_quick.py
```

## 📈 Performance & Optimization

### Database Indexes
```javascript
// Đảm bảo index trên datetime
db.gold_minute_data.createIndex({datetime: 1}, {unique: true, background: true})
```

### Batch Processing
- Batch size: 10,000 records cho extract
- Chunk size: Tự động điều chỉnh theo memory

### Memory Management
- Xử lý data theo chunks
- Clear memory sau mỗi batch
- Monitor memory usage

## 🔄 Upgrade & Maintenance

### Backup Database
```bash
# Backup MongoDB
mongodump --db gold_db --out /path/to/backup

# Restore
mongorestore /path/to/backup
```

### Update Dependencies
```bash
# Update requirements
pip install -r requirements.txt --upgrade

# Test sau khi update
python scripts/test_discord_quick.py
./run.sh restart
```

### Log Cleanup
```bash
# Xóa logs cũ hơn 30 ngày
find . -name "main.log.*" -type f -mtime +30 -delete

# Hoặc nén logs cũ
find . -name "main.log.*.gz" -type f -mtime +30 -exec rm {} \;
```

## 🤝 Contributing

### Development Setup
```bash
# Fork repository
# Clone fork
git clone https://github.com/your-username/gold-data-pipepline.git
cd gold-data-pipepline

# Setup development environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create feature branch
git checkout -b feature/your-feature-name
```

### Code Standards
- Sử dụng type hints
- Viết docstrings cho functions
- Follow PEP 8 style guide
- Test code trước khi commit

### Pull Request Process
1. Update documentation
2. Test đầy đủ functionality
3. Update CHANGELOG.md
4. Create PR với description chi tiết

## 📝 Changelog

### v1.0.0 (Current)
- ✅ Initial release với đầy đủ tính năng
- ✅ Realtime data collection từ TradingView
- ✅ Historical data import từ Google Drive
- ✅ Automatic gap detection và filling
- ✅ Discord alerts integration
- ✅ Log rotation system
- ✅ Service management scripts

## 📞 Support

### Documentation
- 📖 [Discord Alerts Guide](docs/DISCORD_ALERTS.md)
- 📊 [Log Rotation Guide](docs/LOG_ROTATION.md)
- 🔧 [Setup Checklist](DISCORD_CHECKLIST.md)

### Issues & Bugs
- Tạo issue trên GitHub với chi tiết lỗi
- Include logs và error messages
- Mô tả steps để reproduce

### Feature Requests
- Mô tả use case cụ thể
- Giải thích lợi ích
- Đề xuất implementation approach

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Gold Data Pipeline** - Hệ thống thu thập dữ liệu vàng realtime đáng tin cậy với monitoring và alerting toàn diện. 🚀📈