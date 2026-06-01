-- Create Dimension Company Table
CREATE TABLE IF NOT EXISTS dim_company (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    sector VARCHAR(50),
    industry VARCHAR(50)
);

-- Create Dimension Date Table
CREATE TABLE IF NOT EXISTS dim_date (
    date DATE PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Create Fact Stock Price Table
CREATE TABLE IF NOT EXISTS fact_stock_price (
    date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, ticker),
    FOREIGN KEY (ticker) REFERENCES dim_company(ticker) ON DELETE CASCADE,
    FOREIGN KEY (date) REFERENCES dim_date(date) ON DELETE CASCADE
);

-- Create Fact Stock Indicator Table
CREATE TABLE IF NOT EXISTS fact_stock_indicator (
    date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    sma_20 DOUBLE PRECISION,
    sma_50 DOUBLE PRECISION,
    ema_20 DOUBLE PRECISION,
    rsi DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_hist DOUBLE PRECISION,
    bollinger_upper DOUBLE PRECISION,
    bollinger_lower DOUBLE PRECISION,
    PRIMARY KEY (date, ticker),
    FOREIGN KEY (ticker) REFERENCES dim_company(ticker) ON DELETE CASCADE,
    FOREIGN KEY (date) REFERENCES dim_date(date) ON DELETE CASCADE
);

-- Insert metadata for the stock tickers
INSERT INTO dim_company (ticker, company_name, sector, industry) VALUES
('ADRO', 'Adaro Energy Indonesia Tbk', 'Energy', 'Coal Production'),
('ANTM', 'Aneka Tambang Tbk', 'Basic Materials', 'Metals & Mining'),
('ASII', 'Astra International Tbk', 'Industrials', 'Conglomerates'),
('BBCA', 'Bank Central Asia Tbk', 'Financials', 'Banks'),
('BBNI', 'Bank Negara Indonesia (Persero) Tbk', 'Financials', 'Banks'),
('BMRI', 'Bank Mandiri (Persero) Tbk', 'Financials', 'Banks'),
('BRIS', 'Bank Syariah Indonesia Tbk', 'Financials', 'Banks'),
('PGAS', 'Perusahaan Gas Negara Tbk', 'Utilities', 'Gas Utilities'),
('TLKM', 'Telkom Indonesia (Persero) Tbk', 'Telecommunication', 'Telecom Services'),
('UNTR', 'United Tractors Tbk', 'Industrials', 'Heavy Machinery')
ON CONFLICT (ticker) DO UPDATE SET
    company_name = EXCLUDED.company_name,
    sector = EXCLUDED.sector,
    industry = EXCLUDED.industry;
