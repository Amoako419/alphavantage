from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from pendulum import datetime
from dotenv import load_dotenv
import requests
import os
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
load_dotenv()


# The API key
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
# The postgres connection id
postgres_id = 'postgres_id'

# The stock ticker
ticker = "AAPL"

def get_data(**kwargs):
    """ This fuction is used to get data from the API """
    import pandas as pd
    #Visit the api documentation https://www.alphavantage.co/documentation/
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=60min&apikey={API_KEY}'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame.from_dict(data['Time Series (60min)'], orient='index')
    print(df)

    # Push the DataFrame to Xcom
    kwargs['ti'].xcom_push(key='stock_data', value=df)
    return df



def process_data(**kwargs):
    """ This function is used to process the data """
    df = kwargs['ti'].xcom_pull(key='stock_data', task_ids='Get_data')
    #change the column names
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    # Calculate the daily returns
    df['close'] = df['close'].astype(float)
    df['daily_returns'] = df['close'].pct_change()
    df.index.name = 'timestamp'
    kwargs['ti'].xcom_push(key='processed_stock_data', value=df)
    return df



# Define the function to load the data into the database
def load_data(**kwargs):
    df = kwargs['ti'].xcom_pull(key='processed_stock_data', task_ids='process_data')
    # Load the transformed data into the database.
    pg_hook = PostgresHook(postgres_conn_id=postgres_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stocks (
    timestamp TIMESTAMP PRIMARY KEY,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    daily_returns FLOAT
     );
    """)

    
    # Insert transformed data into the table

    cursor.execute("""
        INSERT INTO stadiums_pipeline (timestamp, open, high, low, close, volume, daily_returns)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (
            df['timestamp'],
            df['open'],
            df['high'],
            df['low'],
            df['close'],
            df['volume'],
            df['daily_returns']
        ))

    conn.commit()
    cursor.close()
    
    


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2025, 2, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Bell", "retries": 3},
    tags=["Billy the Kid"],
)
def etl():
    http_sensor_check = HttpSensor(
        task_id="http_sensor_check_async",
        http_conn_id="api_http",
        endpoint="symbol={ticker}&interval=60min&apikey={API_KEY}",
        deferrable=True,
        poke_interval=5
    )

    task_http_get = PythonOperator(
        task_id = "Get_data",
        python_callable = get_data
        )
    
    task_process_data = PythonOperator(
        task_id = "Process_data",
        python_callable = process_data,
        provide_context = True
        )
    
    task_load_data = PythonOperator(
        task_id = "Load_data",
        python_callable = load_data,
        provide_context = True
    )

    
    http_sensor_check >> task_http_get >> task_process_data >> task_load_data
etl()