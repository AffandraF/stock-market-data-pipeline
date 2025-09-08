-- src/utils/init.sql
CREATE TABLE IF NOT EXISTS stock_data (
    date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    SMA_5 DOUBLE PRECISION,
    SMA_20 DOUBLE PRECISION,
    EMA_12 DOUBLE PRECISION,
    RSI_14 DOUBLE PRECISION,
    Bollinger_Upper DOUBLE PRECISION,
    Bollinger_Lower DOUBLE PRECISION,
    year INT,
    month INT,
    PRIMARY KEY (date, ticker)
);
