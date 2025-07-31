# Employee-Attrition-Analysis-with-Automated-Data-Pipeline
# Analisis Atrisi Karyawan dengan Pipeline Data Otomatis


## Repository Outline
`
P2M3_riko_fadilah_ddl.txt          : Berisi url dataset, syntax pembuatan tabel dan insert data 
P2M3_riko_fadilah_data_raw.csv     : Berisi data original dari kaggle.
P2M3_riko_fadilah_data_clean.csv   : Berisi data yang sudah dicleaning.
P2M3_riko_fadilah_DAG.py           : Berisi definisi DAG yang digunakan untuk mengelola alur kerja ETL (Extract, Transform, Load).
P2M3_riko_fadilah_conceptual.txt   : Berisi jawaban dari conceptual problem.
P2M3_riko_fadilah_GX.ipynb         : Berisi notebook Jupyter yang mendokumentasikan validasi dataset dengan menggunakan Great Expectations.
Folder Images                      : Folder berisi screenshot visualisasi dashboard di Kibana.
`

## Problem Background
Tingkat turnover cukup tinggi pada level junior sehingga dapat menyebabkan membengkaknya biaya untuk rekrutment karyawan. Sebagai HR Analyst laporan akan mengungkap faktor atrisi karyawan. Sehingga dapat memberikan rekomendasi perbaikan kebijakan HR seperti penyesuaian jenjang karir atau program work-life balance. 


## Project Output
Output berupa visualisasi hasil analisis dalam bentuk grafik. 


## Data
Dataset berasal dari kaggle dengan url

https://www.kaggle.com/datasets/pavansubhasht/ibm-hr-analytics-attrition-dataset 


## Method
Metode menggunakan ETL Automation: PostgreSQL → Airflow → Elasticsearch, dengan menggunakan Datavalidation dari GreatExpectations, dan visualisasi dengan Kibana.


## Stacks
PostgreSQL, Airflow, Great Expectation, Elasticsearch, dan Kibana


