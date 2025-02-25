from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from pendulum import datetime
import requests
from airflow.operators.python import PythonOperator

ticker = "AAPL"
API_KEY = "0V5NWS6TJDOB1A74"
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
    df = kwargs['ti'].xcom_pull(key='stock_data', task_ids='extract_transform')
    
    


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
    task_http_sensor_check = HttpSensor(
    task_id="http_sensor_check",
    http_conn_id="http_default",
    endpoint="",
    request_params={},
    response_check=lambda response: "httpbin" in response.text,
    poke_interval=5
    )

    task_http_get = PythonOperator(
        task_id = "Get data",
        python_callable = get_data,
        op_args = ['**kwargs'],
        )