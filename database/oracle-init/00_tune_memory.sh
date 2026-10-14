#!/bin/bash
# Chạy MỘT LẦN khi khởi tạo database lần đầu.
# Oracle Free mặc định dùng SGA 1.5G + PGA 512M, dư thừa cho DWH demo: giảm còn
# 1G + 256M (tiết kiệm ~0.7GB RAM). SGA chỉ đổi được khi restart instance nên
# khởi động lại ngay tại đây, trước khi tạo schema.
echo "Giảm bộ nhớ Oracle: SGA 1G, PGA 256M..."
sqlplus -s / as sysdba <<'SQL'
ALTER SYSTEM SET sga_target = 1G SCOPE = SPFILE;
ALTER SYSTEM SET sga_max_size = 1G SCOPE = SPFILE;
ALTER SYSTEM SET pga_aggregate_target = 256M SCOPE = SPFILE;
SHUTDOWN IMMEDIATE
STARTUP
ALTER PLUGGABLE DATABASE ALL OPEN;
SQL
