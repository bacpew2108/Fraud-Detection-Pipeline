# Real-time Fraud Detection Pipeline

Pipeline phát hiện gian lận giao dịch thời gian thực sử dụng Kafka, Spark Streaming, Redis, và Oracle.

## Kiến Trúc

- **Data Generator (producer.py)**: Tạo dữ liệu giao dịch giả và đẩy lên Kafka
- **Redis Cache (init_redis.py)**: Tải profile khách hàng (age, city, risk_segment) làm lookup table
- **Spark Streaming (spark_processor.py)**: Đọc từ Kafka, enrich với Redis, phát hiện fraud, ghi vào Oracle
- **Oracle Database**: Lưu kết quả dưới dạng star schema (fact_transactions + dim tables)

## Prerequisites

- Python 3.8+
- Docker & Docker Compose
- Oracle SQL client (tùy chọn, để kiểm tra database)

## Setup

### 1. Khởi động hạ tầng (Kafka, Redis, Oracle)

```bash
docker compose up -d
```

Kiểm tra các service khởi động thành công:

```bash
docker compose ps
```

### 2. Cài đặt Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Tạo Kafka topic

```bash
# Chờ Kafka sẵn sàng (~10 giây)
docker exec -it kafka kafka-topics --create \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 4. Khởi tạo Oracle schema

```bash
# Kết nối vào Oracle container và chạy SQL
docker exec -i oracle-db sqlplus system/Admin1234@FREEPDB1 < database/init_oracle.sql
```

### 5. Nạp dữ liệu vào Redis cache

```bash
python database/init_redis.py
```

Output mong đợi: "Hoàn tất quá trình nạp dữ liệu tĩnh (Lookup Cache) vào Redis!"

## Chạy Pipeline

### Terminal 1: Chạy Spark Streaming (xử lý + ghi Oracle)

```bash
python streaming_app/spark_processor.py
```

Output: "Successfully wrote X records to Oracle" (mỗi batch)

### Terminal 2: Chạy Data Generator (tạo dữ liệu)

```bash
python data_generator/producer.py
```

Output: "Đã đẩy 500 GD. Mẫu: ..." (mỗi 500 giao dịch)

## Kiểm Tra Kết Quả

### Xem dữ liệu trong Oracle

```bash
docker exec -it oracle-db sqlplus system/Admin1234@FREEPDB1
SQL> SELECT * FROM fact_transactions WHERE rownum <= 10;
SQL> SELECT COUNT(*) FROM fact_transactions;
SQL> SELECT COUNT(*) FROM fact_transactions WHERE is_fraud = 1;
```

### Kiểm tra Redis

```bash
docker exec -it redis_cache redis-cli
> KEYS ACC_*
> GET ACC_00001
```

### Xem Kafka topics

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --from-beginning \
  --max-messages 5
```

## Cấu Hình Môi Trường

File `.env` chứa:
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`: Cấu hình Oracle
- `DB_SERVICE_NAME`: Tên service Oracle (ORCLPDB1)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address
- `REDIS_HOST`, `REDIS_PORT`: Redis connection

Sửa các giá trị này để chạy trên hạ tầng khác.

## Dừng Pipeline

```bash
# Dừng Spark: Ctrl+C ở terminal chạy spark_processor.py
# Dừng Producer: Ctrl+C ở terminal chạy producer.py
# Dừng services:
docker compose down
```

## Troubleshooting

| Lỗi | Nguyên nhân | Cách sửa |
|-----|-----------|---------|
| `Connection refused: localhost:9092` | Kafka chưa sẵn sàng | Chờ 10-15 giây, kiểm tra `docker compose ps` |
| `ORA-12514` | Oracle service chưa khởi động | Chờ ~30 giây cho Oracle boot, kiểm tra logs: `docker logs oracle-db` |
| `Redis connection refused` | Redis chưa khởi động | Kiểm tra: `docker exec redis_cache ping` |
| `No module named 'pyspark'` | Thiếu dependency | Chạy `pip install -r requirements.txt` |
| `Topic 'transactions' does not exist` | Chưa tạo topic | Chạy lệnh tạo topic ở step 3 |

## Architecture Notes

- **Fraud Detection Logic**: 
  - Flag fraud nếu amount = oldbalanceOrg (rút sạch)
  - Flag fraud nếu risk_segment = "High" AND amount > 10000
- **Star Schema**: fact_transactions tham chiếu dim_accounts, dim_time, dim_fraud_reason (thiết kế kho dữ liệu)
- **Checkpoint**: Spark sử dụng `/tmp/spark_checkpoint` để track offset Kafka, tránh reprocessing

## Tài Liệu Liên Quan

- Kafka: https://kafka.apache.org/quickstart
- Spark Streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Oracle: https://www.oracle.com/database/free/