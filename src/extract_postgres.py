import psycopg2
import pandas as pd
import os

# Konfigurasi koneksi Postgres
conn_params = {
    'host': os.getenv("POSTGRES_HOST", "localhost"),
    'port': 5432,
    'database': os.getenv("POSTGRES_DB", "stockdb"),
    'user': os.getenv("POSTGRES_USER", "postgres"),
    'password': os.getenv("POSTGRES_PASSWORD", "postgres")
}

# Membaca data dari tabel stock_data
def fetch_stock_data():
    conn = psycopg2.connect(**conn_params)
    query = "SELECT * FROM stock_data" \
            " ORDER BY date DESC"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Menyimpan ke CSV
if __name__ == "__main__":
    df = fetch_stock_data()
    df.to_csv('/opt/spark-data/stock_data_processed.csv', index=False)