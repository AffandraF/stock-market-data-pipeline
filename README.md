# Stock Market Data Pipeline (Refactored)

An end-to-end production-oriented batch data engineering pipeline that extracts stock market data, validates schema and quality constraints, calculates technical indicators, and loads the processed results into a dimensional data warehouse.

This project is designed to demonstrate data engineering best practices including **modular ETL architecture**, **dimensional modeling (Star Schema)**, **orchestration**, **data quality gates**, and **local OLAP analytics**.

---

## 🚀 Refactored Architecture

The pipeline is refactored from an over-engineered big data stack (Prefect, Kafka, Spark, MinIO, Delta Lake) into a clean, highly maintainable, and cost-effective batch pipeline:

```
                      +-----------------------------+
                      |  Yahoo Finance API / CSVs   |
                      +--------------+--------------+
                                     |
                                     | [Extract] (yfinance API with CSV fallback)
                                     v
                      +-----------------------------+
                      |      Raw Parquet Layer      |
                      |   (data/raw/{ticker}.parquet)|
                      +--------------+--------------+
                                     |
                                     | [Validation] (Schema, nulls, duplicates, bounds)
                                     v
                      +-----------------------------+
                      |   Cleaned Parquet Layer     |
                      | (_validated.parquet)        |
                      +--------------+--------------+
                                     |
                                     | [Transform] (Pandas: SMA, EMA, RSI, MACD, BB)
                                     v
                      +-----------------------------+
                      |   Processed Parquet Layer   |
                      | (data/processed/{ticker}.pq)|
                      +--------------+--------------+
                                     |
                                     | [Load] (Incremental Load / Upsert)
                                     v
                      +-----------------------------+
                      |  PostgreSQL Data Warehouse  |
                      | (Star Schema: Dim/Fact Table)|
                      +--------------+--------------+
                                     |
                                     | [Analytics]
                                     v
                      +-----------------------------+
                      |   DuckDB Analytics Layer    |
                      +-----------------------------+
```

### Key Changes & Rationale:
1. **Airflow (Orchestration)**: Replaced Prefect. Airflow is the industry standard, providing robust scheduling, task dependencies, retries, and a powerful dashboard.
2. **Pandas (Processing)**: Replaced Apache Spark. Spark was severely over-engineered for megabyte-scale datasets. Pandas runs in milliseconds, utilizes negligible RAM, and simplifies indicator math.
3. **Parquet Landing Zones**: Replaced MinIO & Delta Lake. Storing raw and processed stages as local Parquet files simplifies infrastructure while retaining optimized columnar storage.
4. **PostgreSQL Star Schema**: Replaced a single flat table with a star schema design (`dim_company`, `dim_date`, `fact_stock_price`, `fact_stock_indicator`) to follow data warehousing best practices.
5. **yfinance API with local CSV fallback**: Data is pulled directly from Yahoo Finance API. If offline or if the API is restricted, the pipeline automatically falls back to merging historical local CSVs.
6. **DuckDB Analytics**: Integrated DuckDB to allow analysts to query processed Parquet files directly using SQL in milliseconds, bypassing the warehouse database.

---

## 🛠️ Tech Stack
- **Orchestration**: Apache Airflow
- **Data Processing**: Pandas, NumPy
- **Data Quality & Validation**: Custom Python validation gate
- **Data Warehouse**: PostgreSQL (Docker-based)
- **Local OLAP Analytics**: DuckDB
- **Containerization**: Docker & Docker Compose

---

## 📁 Directory Structure
```
stock-market-pipeline/
├── airflow/
│   └── dags/
│       └── stock_pipeline.py      # Airflow DAG definition
├── data/
│   ├── raw/                       # Raw landing zone (CSVs & Parquet)
│   └── processed/                 # Cleaned Parquet files with technical indicators
├── sql/
│   ├── init_warehouse.sql         # Postgres star schema table creations & seeding
│   └── marts.sql                  # PostgreSQL analytics views (marts)
├── src/
│   ├── extract/
│   │   └── extractor.py           # API downloader and CSV fallback merger
│   ├── validate/
│   │   └── validator.py           # Data quality check logic
│   ├── transform/
│   │   └── transformer.py         # Technical indicator calculator using Pandas
│   ├── load/
│   │   └── loader.py              # Incremental database loader (upserts)
│   └── utils/
│       ├── config.py              # Configuration manager via dotenv
│       ├── db.py                  # Database connection utilities
│       ├── logger.py              # Standardized application logging
│       ├── init_db.py             # Automates SQL script execution
│       ├── check_postgres.py      # Query verification utility
│       └── duckdb_analytics.py    # Local DuckDB OLAP queries on Parquet
├── tests/
│   └── test_pipeline.py           # Unit tests (pytest)
├── docker-compose.yml             # Orchestration container profiles
├── requirements.txt               # Project dependencies
└── README.md                      # Documentation
```

---

## 📊 Data Quality & Business Rules
The **Validation Layer** enforces strict data quality gates:
* **Schema Conformity**: Asserts that all required columns (`Date`, `Open`, `High`, `Low`, `Close`, `Volume`) are present and cast to correct numeric and date formats.
* **Duplicate Prevention**: Detects and drops duplicate rows based on the `Date` column.
* **Null Filtering**: Discards records with nulls in any required column.
* **Business Boundary Checks**:
  - Close Price must be strictly positive (`Close > 0`).
  - Volume must be non-negative (`Volume >= 0`).

---

## 📈 Technical Indicators Calculated
Calculated on-the-fly in the **Transform Layer**:
* **SMA 20 & SMA 50**: Simple Moving Averages.
* **EMA 20**: Exponential Moving Average.
* **RSI (14)**: Relative Strength Index.
* **MACD**: Moving Average Convergence Divergence (EMA 12, EMA 26) along with the MACD Signal (EMA 9) and MACD Hist (Histogram).
* **Bollinger Bands**: Standard 20-day window with upper and lower bands set at 2 standard deviations.

---

## 🗄️ Dimensional Model (Star Schema)
Our PostgreSQL database implements a dimensional model configured as follows:

- **`dim_company`** (Dimension Table): Stores ticker metadata (e.g., Sector, Industry, Name). Pre-seeded during initialization.
- **`dim_date`** (Dimension Table): Denormalizes date fields (`day`, `month`, `year`, `quarter`, `day_of_week`, `is_weekend`) for high-performance temporal slicing.
- **`fact_stock_price`** (Fact Table): Stores core price transactions (`open`, `high`, `low`, `close`, `volume`).
- **`fact_stock_indicator`** (Fact Table): Stores corresponding computed technical indicators.

### Data Marts (Self-updating Views):
- `mart_stock_summary`: A flat consolidated view suitable for dashboarding tools (PowerBI, Tableau).
- `mart_weekly_summary`: Aggregates weekly performance metrics (weekly highs, lows, and volume).
- `mart_technical_signals`: Implements business logic indicators (e.g., RSI overbought/oversold alerts, Bollinger breakout triggers).

---

## ⚙️ Setup and Installation

### Prerequisites:
- **Docker** and **Docker Compose** installed.
- **Python 3.10+** (if running tests or scripts locally).

### Step 1: Clone the repository and configure `.env`
```sh
git clone https://github.com/AffandraF/stock-market-data-pipeline.git
cd stock-market-data-pipeline
```
Verify the contents of your `.env` file (which should already be in your workspace):
```env
POSTGRES_DB=stockdb
POSTGRES_USER=warehouse
POSTGRES_PASSWORD=warehouse_password
STOCK_TICKERS=ADRO,ANTM,ASII,BBCA,BBNI,BMRI,BRIS,PGAS,TLKM,UNTR
```

### Step 2: Spin up the containers
This launches PostgreSQL and Apache Airflow services:
```sh
docker compose up -d
```

### Step 3: Access Airflow Dashboard
Open your browser and navigate to:
- **URL**: `http://localhost:8080`
- **Username**: `admin`
- **Password**: `admin`

Trigger the `stock_market_etl_pipeline` DAG manually or let it run daily.

---

## 🔍 Local Validation & Analytics

### Run DuckDB Local OLAP Queries
To run analytical SQL queries directly on the processed Parquet files (bypassing Postgres):
```sh
python src/utils/duckdb_analytics.py
```

### Query PostgreSQL Data Marts
To fetch data loaded in the warehouse database and save the flat table as a CSV file:
```sh
python src/utils/check_postgres.py
```

---

## 🧪 Unit Testing
We use `pytest` to validate transformations and quality constraints. Run them locally:
```sh
pip install -r requirements.txt
pytest tests/
```