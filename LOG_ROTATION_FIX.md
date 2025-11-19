# Sửa lỗi Log Rotation với Nohup

## Vấn đề trước đây

Khi chạy với `nohup`, script đã redirect toàn bộ output vào `main.log`:
```bash
nohup $PYTHON_CMD > "$LOG_FILE" 2>&1 &
```

**Điều này gây ra vấn đề:**
- Python logger có cấu hình `RotatingFileHandler` để tự động xoay vòng log
- Nhưng nohup đang ghi trực tiếp vào file `main.log` 
- Python logger không thể kiểm soát file này → log không xoay vòng
- File log phình to không giới hạn

## Giải pháp đã áp dụng

### 1. Sửa file `run.sh`

**Thay đổi:**
```bash
# TRƯỚC (SAI):
nohup $PYTHON_CMD > "$LOG_FILE" 2>&1 &

# SAU (ĐÚNG):
nohup $PYTHON_CMD > /dev/null 2>&1 &
```

**Lý do:**
- Bỏ redirect vào `main.log`
- Để Python logger hoàn toàn quản lý file `main.log`
- Output của nohup được gửi vào `/dev/null` (bỏ qua)
- Python logger sẽ tự động xoay vòng log theo cấu hình

### 2. Cấu hình Log Rotation hiện tại

File `config/logger_config.py` đã được cấu hình:
```python
RotatingFileHandler(
    filename=base_path,
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=5,  # Giữ 5 file backup
    encoding="utf-8",
)
```

**Cơ chế hoạt động:**
1. Khi `main.log` đạt 10MB → tự động xoay vòng
2. `main.log` → `main.log.1`
3. `main.log.1` → `main.log.2`
4. ...
5. `main.log.5` bị xóa (chỉ giữ 5 backup)
6. Tạo file `main.log` mới

**Tổng dung lượng:** ~60MB (10MB × 6 files)

## Cách sử dụng

### Khởi động service:
```bash
./run.sh start
```

### Kiểm tra log rotation:
```bash
# Xem các file log
ls -lh main.log*

# Kết quả mong đợi:
# main.log      (file đang ghi, < 10MB)
# main.log.1    (backup, ~10MB)
# main.log.2    (backup, ~10MB)
# ...
# main.log.5    (backup cũ nhất, ~10MB)
```

### Dừng service:
```bash
./run.sh stop
```

### Restart service:
```bash
./run.sh restart
```

## Tùy chỉnh nâng cao (Optional)

Nếu muốn nén file backup để tiết kiệm dung lượng, có thể sử dụng `AdvancedLoggerConfig` trong file `config/advanced_logger_config.py`:

```python
from config.advanced_logger_config import AdvancedLoggerConfig

# Thay thế LoggerConfig bằng AdvancedLoggerConfig
logger = AdvancedLoggerConfig.logger_config(
    log_name="MyLogger",
    max_bytes=10 * 1024 * 1024,  # 10MB
    backup_count=5,
    use_compression=True  # Bật nén gzip
)
```

**Lợi ích:**
- File backup được nén thành `.gz` (tiết kiệm ~70-90% dung lượng)
- Ví dụ: `main.log.1.gz`, `main.log.2.gz`, ...

## Kiểm tra hoạt động

```bash
# 1. Khởi động service
./run.sh start

# 2. Theo dõi log realtime
tail -f main.log

# 3. Kiểm tra kích thước file log
watch -n 5 'ls -lh main.log*'

# 4. Khi main.log đạt 10MB, nó sẽ tự động xoay sang main.log.1
```

## Lưu ý quan trọng

1. **Không dùng `tail -f` với nohup redirect:** 
   - ❌ SAI: `nohup python main.py > main.log 2>&1`
   - ✅ ĐÚNG: `nohup python main.py > /dev/null 2>&1` (Python logger tự quản lý)

2. **Python logger phải được cấu hình đúng:**
   - Đảm bảo tất cả module đều import `LoggerConfig`
   - Không dùng `print()` trong production code

3. **File log được tạo tự động:**
   - Không cần tạo `main.log` trước
   - Python logger sẽ tự tạo khi chạy

## Troubleshooting

### Log không xoay vòng?
```bash
# Kiểm tra quyền ghi file
ls -la main.log*

# Kiểm tra process đang chạy
./run.sh status

# Xem log Python có lỗi không
tail -100 main.log
```

### Muốn thay đổi kích thước rotation?
Sửa file `config/logger_config.py`:
```python
maxBytes=20 * 1024 * 1024,  # 20MB thay vì 10MB
backupCount=10,              # Giữ 10 backup thay vì 5
```

### Xem logs cũ (nếu dùng compression)?
```bash
# Giải nén và xem
zcat main.log.1.gz | tail -100

# Hoặc dùng zless
zless main.log.1.gz
```
