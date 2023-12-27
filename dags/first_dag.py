from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
from io import StringIO
from datetime import datetime
import re
import psycopg2
import logging

def transform_read_s3(**kwargs):

    s3 = S3Hook(aws_conn_id='aws_default')
    bucket = 'galendaelt'
    key = 'raw_store_transactions.csv'
    file_obj = s3.get_key(key, bucket_name=bucket)
    file_content = file_obj.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content))
    df = df.to_dict()
    kwargs['ti'].xcom_push(key='transformed_data', value=df)

def push_to_postgres(**kwargs):

    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_read_s3')
    #the xcom works on json, so converting it back to dataframe
    df = pd.DataFrame(transformed_data)

    # the Transformation
    def clean_store_location(st_loc):
        return re.sub(r'[^\w\s]', '', st_loc).strip()
    
    def clean_product_id(pd_id):
        matches = re.findall(r'\d+', pd_id)
        if matches:
            return matches[0]
        return pd_id

    def remove_dollar(amount):
        return float(amount.replace('$', ''))
    
    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: clean_store_location(x))
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: clean_product_id(x))
    
    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: remove_dollar(x))
    
    #Taking just first 100 rows as the whole data(36k rows) is quite heavey for the free tier database instance we are using.
    df = df.head(100)
    logging.info(f"Columns in dataframe: {df.columns}")

    conn = psycopg2.connect(
        host='transactionsdb.cxym4ik0szfd.eu-west-2.rds.amazonaws.com',
        dbname='postgres',
        user='postgres',
        password='password',
        port=5432
    )

    create_table_query = """CREATE TABLE IF NOT EXISTS 
    clean_store_transactions(STORE_ID varchar(50), STORE_LOCATION varchar(50), PRODUCT_CATEGORY varchar(50), 
    PRODUCT_ID int, MRP float, CP float, DISCOUNT float, SP float, DAT date);
    """
    logging.info("clean_store_transactions table has been created")

    insert_query = """
        INSERT INTO clean_store_transactions (STORE_ID, STORE_LOCATION, PRODUCT_CATEGORY, PRODUCT_ID, MRP, CP, DISCOUNT, SP, DAT)
        VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s);
    """
    logging.info(f"Inserted {len(df)} records into clean_store_transactions table.")

    cur = conn.cursor()
    cur.execute(create_table_query)
    try:
        for index, record in df.iterrows():
            cur.execute(insert_query, (record['STORE_ID'], record['STORE_LOCATION'], record['PRODUCT_CATEGORY'],
                                       record['PRODUCT_ID'], record['MRP'], record['CP'], record['DISCOUNT'],
                                       record['SP'], record['Date']))
            
        conn.commit()
    finally:
        # i have included this so that we can confirm that data is indeed stored on database, 
        # we can see this select* output in logs 
        df_read = pd.read_sql('''select * from clean_store_transactions''', conn)
        logging.info(f"{df_read}")
        cur.close()
        conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 9),
}

with DAG('s3_to_postgres_dag', default_args=default_args, schedule_interval=None) as dag:
    transform_read_s3 = PythonOperator(
        task_id='transform_read_s3',
        python_callable=transform_read_s3,
        provide_context=True
        
    )

    push_to_postgres_task = PythonOperator(
        task_id='push_to_postgres',
        python_callable=push_to_postgres,
        provide_context=True
    )
    
    # this next lines specifies the task sequence
    transform_read_s3 >> push_to_postgres_task