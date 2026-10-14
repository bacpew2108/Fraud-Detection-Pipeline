#!/bin/bash
# Chạy tự động MỘT LẦN khi container Oracle khởi tạo database lần đầu (volume trống).
# APP_USER / APP_USER_PASSWORD do image gvenzl/oracle-free tạo sẵn trong FREEPDB1.
echo "Tạo star schema cho user ${APP_USER}..."
sqlplus -s "${APP_USER}/${APP_USER_PASSWORD}@//localhost/FREEPDB1" @/opt/fraud/init_oracle.sql
