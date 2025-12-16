# Cơ chế chống sập Gold Data Pipeline

Tài liệu này mô tả các cơ chế được áp dụng để đảm bảo pipeline không bị dừng khi MongoDB hoặc kết nối gặp sự cố.

## 1. Auto-reconnect MongoDB

**Vị trí**: `config/mongo_config.py`

**Cách hoạt động**:
- Thêm `reset_client()` method để đóng và reset client về None
- Khi gặp lỗi connection trong load/upsert, code sẽ:
  - Không raise exception ra ngoài
  - Gọi `self.mongo_config.reset_client()` 
  - Log thông tin "Will reconnect on next operation"
  - Tiếp tục xử lý (không crash)

**Code example**:
```python
except (ConnectionFailure, ServerSelectionTimeoutError) as e:
    self.logger.error(f"MongoDB connection error: {str(e)}")
    self.mongo_config.reset_client()
    self.logger.info("Will reconnect on next operation")
    # Không raise - tiếp tục loop
```

## 2. Lazy Connection với Health Check

**Vị trí**: `config/mongo_config.py` - `get_client()`

**Cách hoạt động**:
- Client chỉ được tạo khi gọi `get_client()`
- Thêm timeout configuration (5 seconds) để tránh treo lâu
- Test connection với `admin.command('ping')` trước khi return
- Nếu connection fail, return None thay vì raise

**Code example**:
```python
def get_client(self):
    if self._client is None:
        try:
            self._client = MongoClient(**self._config)
            self._client.admin.command('ping')  # Health check
            logger.info("MongoDB connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self._client = None  # Keep None để retry sau
    return self._client
```

## 3. Connection Verification trước mỗi Operation

**Vị trí**: `src/etl/load/realtime_metatrader_load.py` - `_ensure_connection()`

**Cách hoạt động**:
- Trước khi load/upsert data, gọi `_ensure_connection()`
- Method này kiểm tra và tái kết nối nếu cần
- Return False nếu không thể kết nối, method gọi sẽ skip operation
- Không raise exception để pipeline tiếp tục chạy

**Code example**:
```python
def realtime_load(self, df):
    if not self._ensure_connection():
        self.logger.warning("Cannot connect to MongoDB, skipping this batch")
        return 0  # Skip nhưng không crash
    # Tiếp tục load...
```

## 4. Try-except không raise trong Pipeline Loop

**Vị trí**: `src/pipepline/realtime_metatrader_pipepline.py`

**Cách hoạt động**:
- Mọi method trong pipeline loop được wrap trong try-except
- Log error nhưng không raise
- Pipeline tiếp tục chạy dù có lỗi xảy ra
- Đảm bảo service không bị dừng do lỗi tạm thời

**Code example**:
```python
def upsert_current_minute(self):
    try:
        # Logic upsert...
    except Exception as e:
        print(f"Error in upsert_current_minute: {e}")
        # Không raise - để loop tiếp tục chạy
```

## 5. Error Detection và Auto-reset

**Vị trí**: `src/etl/load/realtime_metatrader_load.py`

**Cách hoạt động**:
- Catch cụ thể `ConnectionFailure`, `ServerSelectionTimeoutError`
- Check string error message cho keywords: "closed", "connection"
- Khi detect lỗi connection → reset client ngay lập tức
- Operation tiếp theo sẽ tự động reconnect

**Code example**:
```python
except Exception as e:
    self.logger.error(f"Error: {str(e)}")
    if "closed" in str(e).lower() or "connection" in str(e).lower():
        self.mongo_config.reset_client()
        self.logger.info("Connection lost, will reconnect on next operation")
```

## 6. Timeout Configuration

**Vị trí**: `config/mongo_config.py` - `_init_config()`

**Cách hoạt động**:
- Thêm 3 timeout settings vào MongoDB config:
  - `serverSelectionTimeoutMS: 5000` - Timeout khi chọn server
  - `connectTimeoutMS: 5000` - Timeout khi kết nối
  - `socketTimeoutMS: 5000` - Timeout khi đọc/ghi
- Tránh trường hợp pipeline bị treo vô thời hạn

## Kết quả

**Trước khi áp dụng**:
- MongoDB sập → Pipeline crash → Cần restart manual
- Connection timeout → Process treo
- Network glitch → Service dừng

**Sau khi áp dụng**:
- MongoDB sập → Pipeline log error → Tự reconnect khi MongoDB up
- Connection timeout → Skip operation → Retry lần sau (2 giây)
- Network glitch → Reset client → Auto reconnect ngay lập tức
- **Pipeline chạy liên tục 24/7 không cần restart**

## Testing Scenarios

1. **MongoDB restart**: Pipeline tự reconnect sau 2-5 giây
2. **Network disconnect**: Pipeline log error, skip operation, reconnect khi network up
3. **MongoDB overload**: Pipeline skip slow operations, retry khi server recover
4. **Partial failures**: Một batch fail không ảnh hưởng batches khác

## Monitoring

Theo dõi log để xem các event:
- "MongoDB connected successfully" - Connection thành công
- "MongoDB client reset, will reconnect" - Đã reset, chuẩn bị reconnect
- "Cannot connect to MongoDB, skipping" - Skip operation do không connect được
- "Will reconnect on next operation" - Sẽ retry trong lần gọi tiếp

## So sánh với Symbol Flow Big Data

| Cơ chế | Symbol Flow | Gold Pipeline | Status |
|--------|-------------|---------------|--------|
| Auto-reconnect MongoDB | ✅ | ✅ | Applied |
| Lazy connection | ✅ | ✅ | Applied |
| Try-except không raise | ✅ | ✅ | Applied |
| Connection timeout | ✅ | ✅ | Applied |
| Health check | ✅ | ✅ | Applied |
| Exponential backoff | ✅ | ❌ | Not needed (schedule handles retry) |
| asyncio.gather return_exceptions | ✅ | ❌ | Not needed (không dùng asyncio) |

## Maintenance

Khi thêm feature mới:
1. Wrap DB operations trong try-except
2. Check connection trước khi sử dụng (`_ensure_connection()`)
3. Không raise exception trong pipeline loop
4. Log error với detail để debug
5. Reset client khi gặp connection error
