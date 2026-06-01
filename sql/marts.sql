-- Mart 1: Flat Stock Summary View (for BI tools and dashboarding)
CREATE OR REPLACE VIEW mart_stock_summary AS
SELECT 
    fsp.date,
    fsp.ticker,
    c.company_name,
    c.sector,
    c.industry,
    d.year,
    d.month,
    d.quarter,
    d.day_of_week,
    d.is_weekend,
    fsp.open,
    fsp.high,
    fsp.low,
    fsp.close,
    fsp.volume,
    fsi.sma_20,
    fsi.sma_50,
    fsi.ema_20,
    fsi.rsi,
    fsi.macd,
    fsi.macd_signal,
    fsi.macd_hist,
    fsi.bollinger_upper,
    fsi.bollinger_lower
FROM fact_stock_price fsp
JOIN dim_company c ON fsp.ticker = c.ticker
JOIN dim_date d ON fsp.date = d.date
LEFT JOIN fact_stock_indicator fsi ON fsp.date = fsi.date AND fsp.ticker = fsi.ticker;

-- Mart 2: Weekly Performance Aggregations View
CREATE OR REPLACE VIEW mart_weekly_summary AS
SELECT 
    c.ticker,
    c.company_name,
    d.year,
    DATE_PART('week', fsp.date) AS week_number,
    MIN(fsp.date) AS week_start_date,
    MAX(fsp.date) AS week_end_date,
    AVG(fsp.close) AS avg_close_price,
    MAX(fsp.high) AS weekly_high,
    MIN(fsp.low) AS weekly_low,
    SUM(fsp.volume) AS total_weekly_volume
FROM fact_stock_price fsp
JOIN dim_company c ON fsp.ticker = c.ticker
JOIN dim_date d ON fsp.date = d.date
GROUP BY c.ticker, c.company_name, d.year, DATE_PART('week', fsp.date);

-- Mart 3: Technical Indicators Trading Signals View
CREATE OR REPLACE VIEW mart_technical_signals AS
SELECT 
    date,
    ticker,
    company_name,
    close,
    rsi,
    macd,
    macd_signal,
    bollinger_upper,
    bollinger_lower,
    -- Simple RSI Signals
    CASE 
        WHEN rsi < 30 THEN 'BUY (Oversold)'
        WHEN rsi > 70 THEN 'SELL (Overbought)'
        ELSE 'HOLD'
    END AS rsi_signal,
    -- Bollinger Bands Signals
    CASE 
        WHEN close < bollinger_lower THEN 'BUY (Below Lower Band)'
        WHEN close > bollinger_upper THEN 'SELL (Above Upper Band)'
        ELSE 'HOLD'
    END AS bollinger_signal,
    -- MACD Crossover Signals
    CASE 
        WHEN macd > macd_signal THEN 'BULLISH (MACD Crossover)'
        WHEN macd < macd_signal THEN 'BEARISH (MACD Crossunder)'
        ELSE 'NEUTRAL'
    END AS macd_trend
FROM mart_stock_summary;
