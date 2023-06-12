from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime, timedelta
from pandas import Timestamp
import pandas as pd
import logging
import requests
import json

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract(url):
    # 추출 시점 로깅입력
    logging.info(datetime.utcnow())
    response = requests.get(url)
    data = json.loads(response.text)
    return data

@task
def transform(extract_data):
    records = []
    for data in extract_data:
        country = data["name"]["official"]
        population = data["population"]
        area = data["area"] 
        records.append([country, population, area])
    # transform 끝난 것 확인 
    logging.info("Data Transform finished")
    return records

@task
def load(schema, table, records):
    logging.info("Load Started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        # 테이블 있으면 삭제 실행
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        # 테이블 생성
        cur.execute(f"""
        CREATE TABLE {schema}.{table} (
            country varchar(50),
            population bigint,
            area float
        );""")
        # 모든 record에 대해 삽입 작업 실행
        for record in records:
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s);"
            cur.execute(sql, record)
        cur.execute("COMMIT;")   
    # 에러 발생 시 예외 조건, 롤백 처리
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    # load 작업 완료 확인
    logging.info("Load finished")

# 기본 인자 입력
default_args = {
    'owner': 'taejun3305',
    'email': ['taejun3305@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 생성
with DAG(
    dag_id='UpdateCountriesInfo',
    start_date=datetime(2023, 6, 9),
    catchup=False,
    tags=['API'],
    schedule='30 6 * * 6',
    default_args=default_args
) as dag:

    records = transform(extract("https://restcountries.com/v3/all"))
    load('taejun3305', 'Upload_Countries_Info', records)
