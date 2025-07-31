'''
=================================================
Milestone 3

Nama  : Riko Fadilah
Batch : [Isi Batch Anda]

Program ini adalah DAG Airflow untuk menjalankan pipeline ETL.
Prosesnya meliputi pengambilan data karyawan dari PostgreSQL,
pembersihan data, dan memuatnya ke Elasticsearch.
=================================================
'''
# Import libraries
import datetime as dt1
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch, helpers

# Pengaturan Dasar untuk DAG
default_args = {
    'owner': 'Riko Fadilah Alam', # Owner DAG
    'start_date': datetime(2024, 11, 1), # Penjadwalan mulai DAG
    'retries': 1, # Jumlah percobaan ulang jika task gagal
    'retry_delay': timedelta(minutes=1), # Jeda sebelum mencoba ulang
}


def fetch_from_postgresql():
    """
    Task untuk mengambil data dari PostgreSQL dan menyimpannya sementara.
    """
    # Koneksi ke PostgreSQL
    db_string = 'postgresql://airflow:airflow@postgres:5432/airflow' # Connector string PostgreSQL
    conn = create_engine(db_string)
    df = pd.read_sql("select * from table_m3", conn) # Mengambil data dari tabel 'table_m3'
    # Simpan ke lokasi sementara yang bisa diakses task lain
    df.to_csv('/dags/P2M3_riko_fadilah_data_raw.csv', index=False)

def data_cleaning_and_saving():
    """
    Task untuk membersihkan data mentah dan menyimpannya sebagai file CSV bersih.
    """
    df = pd.read_csv('/dags/P2M3_riko_fadilah_data_raw.csv')

    # Hapus data duplikat 
    df.drop_duplicates(inplace=True)

    # Normalisasi nama kolom 
    # Mengubah ke lowercase, mengganti spasi dengan underscore
    df.columns = [col.lower().replace(' ', '_').strip() for col in df.columns]

    # Handling Missing Values 
    # Isi nilai numerik dengan median dan kategorikal dengan modus
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col].fillna(df[col].median(), inplace=True)
        else:
            df[col].fillna(df[col].mode()[0], inplace=True)
    
    # Menyimpan data bersih ke lokasi yang bisa diakses task selanjutnya
    df.to_csv('/dags/P2M3_riko_fadilah_data_clean.csv', index=False)



def post_to_elasticsearch():
    """
    Task untuk memuat data bersih dari CSV ke Elasticsearch.
    """
    df = pd.read_csv('/dags/P2M3_riko_fadilah_data_clean.csv')

    # Koneksi ke Elasticsearch
    es = Elasticsearch(hosts="http://host.docker.internal:9200")
    
    # Konversi DataFrame ke format JSON untuk bulk insert
    actions = [
        {
            "_index": "milestone3_employees", # Nama index di Elasticsearch
            "_id": record['id'], # Gunakan ID unik dari DataFrame sebagai ID dokumen
            "_source": record
        }
        for record in df.to_dict(orient='records')
    ]
    
    # Lakukan bulk insert untuk efisiensi
    helpers.bulk(es, actions)



# --- Inisialisasi DAG ---
with DAG(
    'P2M3_riko_fadilah_pipeline',
    default_args=default_args,
    description='Pipeline ETL Data Karyawan untuk Milestone 3',
    schedule_interval='10,20,30 9 * * 6', # Menjadwalkan setiap Sabtu pukul 09:10, 09:20, dan 09:30
    start_date=datetime(2024, 11, 1) + timedelta(hours=7),   # Tanggal mulai DAG dan penyesuaian zona waktu karena saya berada di zona waktu UTC+7
    catchup=False,
    tags=['milestone3'],
) as dag:
    
    # Fetch data dari PostgreSQL
    # Menggunakan PythonOperator untuk menjalankan fungsi Python
    fetch_task = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=fetch_from_postgresql,
    )
    # Task untuk membersihkan data dan menyimpannya
    clean_task = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning_and_saving,
    )
    # Task untuk memuat data bersih ke Elasticsearch
    post_task = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch,
    )
    
    # Mengatur urutan eksekusi task
    fetch_task >> clean_task >> post_task