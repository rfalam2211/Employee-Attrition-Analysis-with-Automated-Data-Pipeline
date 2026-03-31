# 📊 Employee Attrition Analysis with Automated Data Pipeline

## 🔍 Latar Belakang

Tingkat turnover karyawan yang tinggi, terutama di level junior, menyebabkan membengkaknya biaya rekrutmen dan hilangnya transfer pengetahuan. Proyek ini bertujuan mengungkap faktor-faktor utama penyebab atrisi karyawan dan memberikan rekomendasi perbaikan kebijakan HR melalui pendekatan data-driven menggunakan pipeline data otomatis.

## 🎯 Tujuan

Menganalisis hubungan antara **Job Satisfaction**, **Monthly Income**, **Years at Company**, dan **Job Role** terhadap atrisi karyawan, serta membangun automated data pipeline untuk memproses dan memvisualisasikan data secara efisien.

## ⚙️ Arsitektur & Tech Stack

| Komponen | Teknologi |
|---|---|
| Database | PostgreSQL |
| Orchestration | Apache Airflow (DAG) |
| Data Validation | Great Expectations |
| Search & Analytics | Elasticsearch |
| Visualization | Kibana |

**Pipeline Flow:**
```
PostgreSQL → Apache Airflow (ETL) → Elasticsearch → Kibana Dashboard
```

Pipeline dijadwalkan berjalan otomatis setiap hari Sabtu melalui Airflow DAG dengan 3 task berurutan:
1. **Fetch** — Mengambil data dari PostgreSQL
2. **Clean** — Membersihkan data (duplikat, normalisasi kolom, handling missing values)
3. **Load** — Memuat data bersih ke Elasticsearch untuk divisualisasikan di Kibana

## 📁 Struktur File

```
├── P2M3_riko_fadilah_DAG.py            : Definisi DAG Airflow untuk pipeline ETL otomatis
├── P2M3_riko_fadilah_GX.ipynb          : Validasi kualitas data dengan Great Expectations (7 rules)
├── P2M3_riko_fadilah_data_raw.csv      : Dataset mentah (1.470 records, 35 fitur)
├── P2M3_riko_fadilah_data_clean.csv    : Dataset setelah proses cleaning
├── P2M3_riko_fadilah_ddl.ipynb         : DDL & referensi dataset
├── P2M3_riko_fadilah_DAG_graph.png     : Visualisasi graph DAG Airflow
└── Images/                             : Screenshot dashboard Kibana
```

## 📋 Data Validation (Great Expectations)

Diterapkan **7 aturan validasi** yang seluruhnya **lulus (100% success)**:

| # | Validasi | Kolom | Hasil |
|---|---|---|---|
| 1 | Keunikan primary key | `employeenumber` | ✅ Pass |
| 2 | Rentang usia kerja (18–65) | `age` | ✅ Pass |
| 3 | Konsistensi nilai kategorikal | `gender` | ✅ Pass |
| 4 | Tipe data numerik | `monthlyincome` | ✅ Pass |
| 5 | Panjang karakter wajar (5–50) | `jobrole` | ✅ Pass |
| 6 | Rata-rata kenaikan gaji (10–25%) | `percentsalaryhike` | ✅ Pass (15.2%) |
| 7 | Format regex flag biner | `over18` | ✅ Pass |

## 📈 Temuan Utama

### 1. Peran dengan Atrisi Tertinggi
Laboratory Technician **(62)**, Sales Executive **(57)**, dan Research Scientist **(47)** mendominasi kasus atrisi — mengindikasikan tekanan kerja tinggi pada peran teknis dan penjualan.

### 2. Distribusi Usia Atrisi
Konsentrasi atrisi tertinggi pada rentang **usia 26–35 tahun** (fase awal–pertengahan karir), menunjukkan kesulitan perusahaan mempertahankan talenta muda produktif.

### 3. Pengaruh Overtime
**53.59%** karyawan yang atrisi adalah mereka yang sering bekerja lembur — membuktikan burnout dan ketidakseimbangan work-life balance sebagai faktor pendorong utama.

### 4. Gaji vs Kepuasan vs Level Jabatan
Heatmap menunjukkan gaji tinggi **tidak menjamin** kepuasan kerja. Karyawan di Job Level 4–5 dengan gaji tertinggi masih melaporkan kepuasan rendah — faktor **non-finansial** (lingkungan kerja, pengakuan, tantangan) lebih dominan.

### 5. Masa Kerja & Atrisi
Puncak atrisi terjadi pada **< 2 tahun** masa kerja, mengindikasikan kegagalan proses onboarding atau ekspektasi yang tidak sesuai realitas pekerjaan.

### 6. Income vs Total Working Years
Zona bahaya atrisi: karyawan dengan **masa kerja < 10 tahun** dan **pendapatan < $7.500**. Karyawan senior (> 20 tahun, gaji > $15.000) memiliki retensi sangat tinggi.

## 💡 Rekomendasi

- **Program onboarding terstruktur** untuk menekan atrisi di 2 tahun pertama
- **Technical career ladder** untuk peran Laboratory Technician & Research Scientist
- **Audit beban kerja & kebijakan overtime** untuk mencegah burnout
- **Stay Interview** proaktif untuk karyawan senior dan talenta kunci
- **Penyesuaian kompensasi kompetitif** untuk karyawan level awal
- **Program akselerasi karir** agar karyawan usia 26–35 tidak merasa stagnan

## 📊 Data

Dataset: [IBM HR Analytics Employee Attrition & Performance](https://www.kaggle.com/datasets/pavansubhasht/ibm-hr-analytics-attrition-dataset) (Kaggle)
— 1.470 records | 35 fitur
