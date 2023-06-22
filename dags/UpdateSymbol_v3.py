from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp
import yfinance as yf
import pandas as pd
import logging

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@ task
def get_historical_prices(symbol):
    ticket = yf.Ticker(symbol)
    data = ticket.history()
    records = []

    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d %H:%M:%S')
        records.append([date, row['Open'], row['High'], row['Low'], row['Close'], row['Volume']])
        return records

def create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLES IF EXISTS {schema}.{table};")
    cur.execute(f"""
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    date date,
    "open" float,
    high float,
    low float,
    close float,
    volume bigint,
    created_date timestamp DEFAULT GETDATE()
);""")

@task
def load(schema, table, records):
    logging.info('load started')
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        # 원본 테이블 없으면 생성
        _create_table(cur, schema, table, False)
        # 임시 테이블을 원본테이블로 복사
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        for r in records:
            sql = f"INSERT INTO t VALUES ('{r[0]}', {r[1]}, {r[2]}, {r[3]}, {r[4]}, {r[5]});"
            print(sql)
            cur.execute(sql)
        # 임시테이블을 원본테이블블로 복사
        cur.execute(f"DELETE FROM {schema}.{table}")
        cur.execute(f"""
INSERT INTO {schema}.{table}
SELECT date, "open", high, low, close, volume FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
    FROM t
)
HWERE seq = 1; """)
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info('load finished')

with DAG(
    dag_id = 'UpdateSymbol_v3',
    start_date = datetime(2022, 5, 30),
    catchup=False,
    tags=['API'],
    schedule='0 10 * * *'
) as dag:

    results = get_historical_prices("AAPL")
    load("taejun3305", 'AAPL_Stock_Info_v3', results)