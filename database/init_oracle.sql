-- =========================================
-- DIM TABLES
-- =========================================

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE fact_transactions PURGE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE dim_fraud_reason PURGE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE dim_time PURGE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE dim_accounts PURGE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE seq_transaction';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -2289 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE seq_fraud_reason';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -2289 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE seq_time';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -2289 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE seq_account';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -2289 THEN
            RAISE;
        END IF;
END;
/

CREATE TABLE dim_accounts (
    account_id        NUMBER PRIMARY KEY,
    age               NUMBER,
    home_city         VARCHAR2(100),
    risk_segment      VARCHAR2(50)
);

CREATE TABLE dim_time (
    time_id        NUMBER PRIMARY KEY,
    hour           NUMBER,
    day            NUMBER,
    month          NUMBER,
    year           NUMBER,
    day_of_week    VARCHAR2(10)
);

CREATE TABLE dim_fraud_reason (
    fraud_reason_id   NUMBER PRIMARY KEY,
    reason_name       VARCHAR2(255),
    risk_level        VARCHAR2(50)
);

-- =========================================
-- FACT TABLE
-- =========================================

CREATE TABLE fact_transactions (
    transaction_id    NUMBER PRIMARY KEY,
    account_id        NUMBER,
    time_id           NUMBER,
    fraud_reason_id   NUMBER,
    amount            NUMBER(15,2),
    is_fraud          NUMBER(1)
);

-- =========================================
-- INDEXES
-- =========================================

CREATE INDEX idx_fact_account ON fact_transactions(account_id);
CREATE INDEX idx_fact_time ON fact_transactions(time_id);
CREATE INDEX idx_fact_fraud ON fact_transactions(fraud_reason_id);

-- =========================================
-- SEQUENCES
-- =========================================

CREATE SEQUENCE seq_account START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_time START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_fraud_reason START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_transaction START WITH 1 INCREMENT BY 1;