PYTHON ?= python3

.PHONY: help install env up init-redis spark producer alerts sql test reset-oracle down clean

help:          ## Liệt kê các lệnh
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-13s %s\n", $$1, $$2}'

install:       ## Cài Python dependencies (kèm pytest)
	$(PYTHON) -m pip install -r requirements-dev.txt

env:           ## Tạo .env từ .env.example (nếu chưa có)
	@test -f .env && echo ".env đã tồn tại" || (cp .env.example .env && echo "Đã tạo .env, hãy đổi mật khẩu trong đó")

up:            ## Khởi động Kafka, Redis, Oracle và chờ tới khi healthy (lần đầu Oracle mất 1-3 phút)
	docker compose up -d --wait kafka redis oracle-db
	docker compose up kafka-init

init-redis:    ## Nạp profile khách hàng vào Redis
	$(PYTHON) database/init_redis.py

spark:         ## Chạy Spark Streaming (Terminal 1)
	$(PYTHON) streaming_app/spark_processor.py

producer:      ## Chạy data generator (Terminal 2)
	$(PYTHON) data_generator/producer.py

alerts:        ## Xem các cảnh báo gian lận trên topic fraud_alerts
	docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 \
		--topic fraud_alerts --property print.key=true

sql:           ## Mở SQL*Plus vào DWH bằng user ứng dụng
	docker exec -it oracle-db bash -c 'sqlplus "$$APP_USER/$$APP_USER_PASSWORD@//localhost/FREEPDB1"'

test:          ## Chạy unit test
	$(PYTHON) -m pytest -q

reset-oracle:  ## Tạo lại star schema (XÓA toàn bộ dữ liệu DWH)
	docker exec -i oracle-db bash -c 'sqlplus -s "$$APP_USER/$$APP_USER_PASSWORD@//localhost/FREEPDB1"' < database/init_oracle.sql

down:          ## Dừng các container (giữ dữ liệu)
	docker compose down

clean:         ## Dừng container, XÓA volume và checkpoint Spark
	docker compose down -v
	rm -rf streaming_app/.checkpoint
