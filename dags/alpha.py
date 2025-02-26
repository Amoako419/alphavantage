from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from pendulum import datetime
from dotenv import load_dotenv
import requests
import os
import json
import pandas as pd
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
    """ This function is used to get data from the API """
    #Visit the api documentation https://www.alphavantage.co/documentation/
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=60min&month=2009-01&outputsize=full&apikey={API_KEY}'
    r = requests.get(url)
    data = r.json()
    
    # Check if the expected data is present
    if 'Time Series (60min)' not in data:
        raise ValueError(f"Expected 'Time Series (60min)' in response, got: {data.keys()}")
        
    df = pd.DataFrame.from_dict(data['Time Series (60min)'], orient='index', dtype=float)
    df.reset_index(inplace=True)
    print(df)

    # Convert DataFrame to a dictionary for XCom
    df_dict = df.to_dict(orient='records')
    
    # Push the DataFrame to XCom
    kwargs['ti'].xcom_push(key='stock_data', value=df_dict)
    return df_dict



def process_data(**kwargs):
    """ This function is used to process the data """
    df_dict = kwargs['ti'].xcom_pull(key='stock_data', task_ids='Get_data')
    
    if df_dict is None:
        raise ValueError("No data received from previous task")
        
    # Convert back to DataFrame
    df = pd.DataFrame(df_dict)
    
    # Rename columns - assuming the first column is 'index' after reset_index
    df.columns = ['timestamp', '1. open', '2. high', '3. low', '4. close', '5. volume']
    
    # Clean column names
    df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    
    # Calculate the daily returns
    df['close'] = df['close'].astype(float)
    df['daily_returns'] = df['close'].pct_change()
    
    # Remove NaN values or convert to a specific value
    df = df.fillna(0)
    
    # Convert back to dictionary for XCom
    df_dict = df.to_dict(orient='records')
    
    kwargs['ti'].xcom_push(key='processed_stock_data', value=df_dict)
    return df_dict



# Define the function to load the data into the database
def load_data(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(key='processed_stock_data', task_ids='Process_data')
    
    if df_dict is None:
        raise ValueError("No processed data received from previous task")
        
    # Convert back to DataFrame
    df = pd.DataFrame(df_dict)
    
    # Load the transformed data into the database
    pg_hook = PostgresHook(postgres_conn_id=postgres_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stocks_full (
    timestamp VARCHAR(255) PRIMARY KEY,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    daily_returns FLOAT
     );
    """)

    # Insert rows one by one
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stocks_full (timestamp, open, high, low, close, volume, daily_returns)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) 
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                daily_returns = EXCLUDED.daily_returns;
            """, (
                row['timestamp'],
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                float(row['volume']),
                float(row['daily_returns'])
            ))

    conn.commit()
    cursor.close()
    conn.close()
    


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2025, 2, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Bell", "retries": 3},
    tags=["Billy the Kid"],
)
def etl():
    http_sensor_check = HttpSensor(
        task_id="http_sensor_check_async",
        http_conn_id="api_http",
        endpoint=f"query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=5min&month=2009-01&outputsize=full&apikey={API_KEY}",
        deferrable=True,
        poke_interval=5
    )

    task_http_get = PythonOperator(
        task_id = "Get_data",
        python_callable = get_data
    )
    
    task_process_data = PythonOperator(
        task_id = "Process_data",
        python_callable = process_data
    )
    
    task_load_data = PythonOperator(
        task_id = "Load_data",
        python_callable = load_data
    )

    
    http_sensor_check >> task_http_get >> task_process_data >> task_load_data

etl()