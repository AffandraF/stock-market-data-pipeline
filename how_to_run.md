# Panduan Menjalankan Proyek Stock Market Data Pipeline

Panduan ini menjelaskan langkah-langkah untuk menjalankan, memantau, dan menganalisis data pada pipeline data pasar saham ini setelah dilakukan restrukturisasi direktori.

---

## 📋 Prasyarat
Sebelum memulai, pastikan perangkat Anda telah terinstal:
1. **Docker** dan **Docker Compose**
2. **Python 3.10+** (opsional, jika ingin menjalankan test atau kueri analitik lokal langsung di terminal komputer Anda)

---

## ⚙️ Opsi Konfigurasi Ekstraksi (API / CSV / Hybrid)
Proses penarikan data mentah pada `src/extract.py` mendukung tiga mode ekstraksi data yang dapat dikonfigurasi secara fleksibel:
* **`api`**: Hanya mengambil data secara *real-time* dari Yahoo Finance API. Jika API gagal/kosong, program akan langsung mengembalikan *error* (tanpa fallback).
* **`csv`**: Mengabaikan panggilan API dan hanya membaca data lokal dari file CSV yang ada di folder data mentah (`{ticker}_history.csv` dan `{ticker}_kafka.csv`).
* **`hybrid`** (Default): Mencoba mengunduh dari Yahoo Finance API terlebih dahulu. Jika gagal (karena batas kuota atau offline), program akan otomatis menggunakan fallback CSV lokal.

### Cara Konfigurasi:
Ubah nilai variabel `EXTRACTION_MODE` pada file `.env` di direktori root proyek Anda:
```env
EXTRACTION_MODE=hybrid  # Opsi: api, csv, hybrid
```

---

## 🚀 Langkah 1: Setup Lingkungan Lokal (Opsional)
Jika Anda ingin menjalankan pengujian unit atau analisis lokal menggunakan Python bawaan komputer Anda, instal dependensi yang diperlukan:
```sh
pip install -r requirements.txt
```

---

## 🐳 Langkah 2: Menyalakan Layanan dengan Docker
Jalankan Docker Compose untuk mengaktifkan database PostgreSQL dan Apache Airflow:
```sh
docker compose up -d
```
> [!NOTE]
> Perintah ini akan mengunduh image yang dibutuhkan (jika belum ada) dan menjalankan layanan di latar belakang (*detached mode*).
> Proses inisialisasi awal database (`airflow-init`) akan berjalan otomatis untuk melakukan migrasi database Airflow dan membuat user admin pertama kali.

---

## 📅 Langkah 3: Menjalankan Pipeline di Dashboard Airflow
Setelah semua kontainer menyala, ikuti langkah berikut:

1. Buka browser Anda dan navigasikan ke alamat: **[http://localhost:8080](http://localhost:8080)**
2. Masuk menggunakan kredensial default:
   * **Username**: `admin`
   * **Password**: `admin`
3. Cari DAG dengan nama **`stock_market_etl_pipeline`**.
4. Aktifkan DAG dengan mengklik tombol *toggle switch* di sebelah kiri nama DAG agar berubah menjadi warna biru (**Active**).
5. Klik ikon **Play (Trigger DAG)** di sebelah kanan untuk menjalankan pipeline secara manual sekarang juga.
6. Anda dapat memantau visualisasi grafis status jalannya task (`initialize_database` -> `extract` -> `validate` -> `transform` -> `load` -> `verify_data_marts`) secara langsung.

---

## 📊 Langkah 4: Menjalankan Kueri Analisis Data
Setelah status DAG Airflow selesai dengan sukses (berwarna hijau), Anda dapat menganalisis data dengan dua cara:

### A. Analisis Cepat Lokal dengan DuckDB (Membaca file Parquet langsung)
Gunakan DuckDB untuk membaca data dari folder `data/processed/` tanpa perlu melakukan query ke database PostgreSQL:
```sh
python src/analytics.py
```
Ini akan mengeksekusi kueri agregasi secara cepat dan menampilkan ringkasan data historis serta sinyal trading RSI di terminal.

### B. Mengambil Data Ringkasan dari PostgreSQL Warehouse
Jalankan skrip berikut untuk mengambil baris data dari view `mart_stock_summary` PostgreSQL dan menyimpannya sebagai file CSV lokal:
```sh
python utils/check_postgres.py
```
Hasil ekspor akan tersimpan di file `data/stock_data_processed.csv`.

---

## 🧪 Langkah 5: Menjalankan Unit Tests (Opsional)
Untuk memastikan seluruh fungsi validasi dan perhitungan indikator berjalan dengan benar:
```sh
pytest tests/
```
