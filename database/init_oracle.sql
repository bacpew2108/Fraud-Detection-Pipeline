-- =========================================
-- Star schema cho Fraud Detection DWH
--   Chạy với user ứng dụng (DB_USER) trên FREEPDB1.
--   Tự động chạy lần đầu container Oracle khởi tạo (database/oracle-init/),
--   chạy lại thủ công bằng: make reset-oracle  (XÓA toàn bộ dữ liệu)
-- =========================================
WHENEVER SQLERROR EXIT SQL.SQLCODE

-- Xóa bảng cũ nếu có (CASCADE CONSTRAINTS nên không phụ thuộc thứ tự foreign key)
BEGIN
    FOR t IN (
        SELECT table_name FROM user_tables
        WHERE table_name IN ('FACT_TRANSACTIONS', 'DIM_ACCOUNTS', 'DIM_TIME', 'DIM_FRAUD_REASON')
    ) LOOP
        EXECUTE IMMEDIATE 'DROP TABLE ' || t.table_name || ' CASCADE CONSTRAINTS PURGE';
    END LOOP;

    -- Sequence của phiên bản cũ, không còn dùng
    FOR s IN (
        SELECT sequence_name FROM user_sequences
        WHERE sequence_name IN ('SEQ_ACCOUNT', 'SEQ_TIME', 'SEQ_FRAUD_REASON', 'SEQ_TRANSACTION')
    ) LOOP
        EXECUTE IMMEDIATE 'DROP SEQUENCE ' || s.sequence_name;
    END LOOP;
END;
/

-- =========================================
-- DIM TABLES
-- =========================================

CREATE TABLE dim_accounts (
    account_id        VARCHAR2(20) PRIMARY KEY,     -- ACC_00001
    age               NUMBER(3),
    home_city         VARCHAR2(100),
    risk_segment      VARCHAR2(20) NOT NULL         -- Low / Medium / High / Unknown
);

-- Grain theo giờ; time_id là smart key YYYYMMDDHH nên không cần sequence
-- và không cần SELECT lại sau khi MERGE
CREATE TABLE dim_time (
    time_id        NUMBER(10) PRIMARY KEY,
    hour           NUMBER(2)    NOT NULL,
    day            NUMBER(2)    NOT NULL,
    month          NUMBER(2)    NOT NULL,
    year           NUMBER(4)    NOT NULL,
    day_of_week    VARCHAR2(10) NOT NULL
);

-- Được seed từ streaming_app/fraud_rules.py khi spark_processor khởi động
CREATE TABLE dim_fraud_reason (
    fraud_reason_id   NUMBER(3) PRIMARY KEY,
    reason_name       VARCHAR2(100) NOT NULL,
    risk_level        VARCHAR2(20)  NOT NULL
);

-- =========================================
-- FACT TABLE
-- =========================================

CREATE TABLE fact_transactions (
    transaction_id    VARCHAR2(36)  PRIMARY KEY,    -- UUID từ nguồn, giúp MERGE idempotent
    account_id        VARCHAR2(20)  NOT NULL,
    dest_account_id   VARCHAR2(20),
    time_id           NUMBER(10)    NOT NULL,
    fraud_reason_id   NUMBER(3)     NOT NULL,
    transaction_type  VARCHAR2(20),
    amount            NUMBER(15,2)  NOT NULL,
    fraud_score       NUMBER(5,4)   NOT NULL,
    is_fraud          NUMBER(1)     NOT NULL,
    event_ts          TIMESTAMP     NOT NULL,       -- thời điểm giao dịch (APP_TIMEZONE)
    ingested_at       TIMESTAMP,                    -- thời điểm message vào Kafka
    processed_at      TIMESTAMP     NOT NULL,       -- thời điểm ghi vào DWH
    CONSTRAINT ck_fact_is_fraud CHECK (is_fraud IN (0, 1)),
    CONSTRAINT fk_fact_account FOREIGN KEY (account_id)      REFERENCES dim_accounts (account_id),
    CONSTRAINT fk_fact_time    FOREIGN KEY (time_id)         REFERENCES dim_time (time_id),
    CONSTRAINT fk_fact_reason  FOREIGN KEY (fraud_reason_id) REFERENCES dim_fraud_reason (fraud_reason_id)
);

-- =========================================
-- INDEXES
-- =========================================

CREATE INDEX idx_fact_account ON fact_transactions(account_id);
CREATE INDEX idx_fact_time ON fact_transactions(time_id);
CREATE INDEX idx_fact_fraud ON fact_transactions(fraud_reason_id);

EXIT
