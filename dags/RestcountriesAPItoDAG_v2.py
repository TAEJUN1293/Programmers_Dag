import requests
import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import psycopg2


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract_transform():
    url = "https://restcountries.com/v3/all"
    response = requests.get(url)
    countries = response.json()
    records = []

    for country in countries:
        name = country['name']['common']
        population = country['population']
        area = country['area']
        
        records.append(['name', 'population', 'area'])
    return records

@task
def load(schema, table, records):
    logging.info("Load Started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
        CREATE TABLE {schema}.{table} (
            country varchar(256),
            population int,
            area float
        );""")
        for record in records:
            # 따옴표 처리를 위해 파라미터 바인딩 사용
            sql = f"INSERT INTO {schema}.{table} VALUES ('{record[0]}', {record[1]}, {record[2]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("Load finished")


with DAG(
    dag_id = 'CountryInfo',
    start_date = datetime(2023, 5, 30),
    catchup = False,
    tags=['API'],
    schedule = '30 6 * * 6',
) as dag:

    results = extract_transform()
    load("taejun3305", "country_info", results)
