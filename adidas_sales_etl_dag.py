# Import libraries
import datetime as dt
from datetime import datetime, timedelta
from airflow import DAG
from elasticsearch import Elasticsearch
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db

# Ambil data dari PostgreSQL dan simpan ke CSV
def get_data_from_db():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("SELECT * FROM table_adidas LIMIT 10000", conn)
    df.to_csv('/opt/airflow/dags/dataset_raw.csv', index=False)
    print("Data berhasil diambil dari PostgreSQL dan disimpan ke dataset_raw.csv")

# Fungsi untuk preprocessing (menerima input dataframe)
def data_preprocessing():
    # Load raw CSV
    df_data = pd.read_csv('/opt/airflow/dags/dataset_raw.csv')

    # Rename kolom ke lowercase dan snake_case
    df_data.columns = [
        'retailer', 'retailer_id', 'invoice_date', 'region', 'state', 'city',
        'product', 'price_per_unit', 'units_sold', 'total_sales',
        'operating_profit', 'operating_margin', 'sales_method'
    ]

    # Bersihkan simbol pada kolom yang sudah di-rename
    df_data['price_per_unit'] = df_data['price_per_unit'].str.replace('$', '').str.replace('.00 ', '')
    df_data['units_sold'] = df_data['units_sold'].str.replace(',', '')
    df_data['total_sales'] = df_data['total_sales'].str.replace('$', '').str.replace(',', '')
    df_data['operating_profit'] = df_data['operating_profit'].str.replace('$', '').str.replace(',', '')
    df_data['operating_margin'] = df_data['operating_margin'].str.replace('%', '')

    # Konversi tipe data
    df_data['price_per_unit'] = df_data['price_per_unit'].astype(int)
    df_data['units_sold'] = df_data['units_sold'].astype(int)
    df_data['total_sales'] = df_data['total_sales'].astype(int)
    df_data['operating_profit'] = df_data['operating_profit'].astype(int)
    df_data['operating_margin'] = df_data['operating_margin'].astype(float)
    df_data['invoice_date'] = pd.to_datetime(df_data['invoice_date'])

    # Simpan hasil cleaning
    df_data.to_csv('/opt/airflow/dags/dataset_clean.csv', index=False)



# Posting ke Elasticsearch
def post_to_elasticsearch():
    es = Elasticsearch("http://elasticsearch:9200")
    if not es.ping():
        print("Tidak bisa terhubung ke Elasticsearch.")
        return

    df = pd.read_csv('/opt/airflow/dags/dataset_clean.csv')
    for i, row in df.iterrows():
        doc = row.to_json()
        es.index(index="table_adidas", id=i + 1, body=doc)

    print("Data berhasil diposting ke Elasticsearch!")

# DAG Setup
default_args = {
    'owner': 'cana',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('adidas_sales_etl_dag',
         description='Global Adidas Sales ETL Pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=dt.datetime(2024, 6, 23) + timedelta(hours=7),  # agar sinkron dengan timezone lokal
         catchup=False) as dag:

    fetch_task = PythonOperator(
        task_id='get_data_from_db',
        python_callable=get_data_from_db
    )

    clean_task = PythonOperator(
        task_id='cleaning_data',
        python_callable=data_preprocessing
    )

    post_task = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

    fetch_task >> clean_task >> post_task
