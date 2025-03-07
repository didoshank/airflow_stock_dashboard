from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from datetime import datetime
from airflow.sensors.python import PythonSensor
from minio import Minio
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table,Metadata



symbol = 'NVDA'


def is_api_available():
    import requests

    api = BaseHook.get_connection('stock_api')
    url = f"{api.host}{api.extra_dejson['endpoint']}"
    print(url)
    response = requests.get(url,headers=api.extra_dejson['headers'])

    try:
        response_json = response.json()
        if(response_json['finance']['result'] is None):
            return url
    
    except (requests.exceptions.RequestException, KeyError, ValueError) as e:
        print(f"Error checking API availability: {e}")
        return False 

def get_stock_prices(url,symbol):
    import requests
    import json
    api = BaseHook.get_connection('stock_api')
    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
    print(url)

  
    response = requests.get(url, headers=api.extra_dejson['headers'])
    #response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
    data = response.json()
    return json.dumps(data['chart']['result'][0])


def store_prices(stock):
    import json
    from io import BytesIO
    minio = BaseHook.get_connection('minio')
    print(minio.extra_dejson['endpoint_url'].split('//')[1])
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key = minio.password,
        secure=False
    )
    bucket_name = 'stock-market'

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    stock = json.loads(stock)  
    symbol = stock['meta']['symbol']
    print(stock,symbol)

    data = json.dumps(stock,ensure_ascii=False).encode('utf8')

    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )

    return f'{objw.bucket_name}/{symbol}'

def get_formatted_csv(path):
    minio = BaseHook.get_connection('minio')
    print(minio.extra_dejson['endpoint_url'].split('//')[1])

    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key = minio.password,
        secure=False
    )

    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    print("Successfully Retrieved the path:"f"{prefix_name}")

    objects = client.list_objects(bucket_name='stock-market',prefix=prefix_name,recursive=True)

    for obj in objects:
        print(obj.object_name)
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    return "BigErrorTime"

with DAG(
    dag_id='stock_market',                
    start_date = datetime(2025,1,1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market']
)as dag:

    is_api_available = PythonOperator(
        task_id='is_api_available',
        python_callable=is_api_available,
        #provide_context=True, # Deprecated, but often still seen in examples
        #op_kwargs={'some_arg': 'some_value'} # Example of passing arguments if needed
    )

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=get_stock_prices,
        op_kwargs={'url':'{{ti.xcom_pull(task_ids="is_api_available")}}','symbol':symbol}
        #provide_context=True, # Deprecated, but often still seen in examples
    )

    store_prices = PythonOperator(
        task_id = 'store_prices',
        python_callable=store_prices,
        op_kwargs={'stock':'{{ti.xcom_pull(task_ids="get_stock_prices")}}'} 
    )

    format_prices = DockerOperator(
        task_id = 'format_prices',
        image = 'airflow/stock-app',
        container_name = 'format_prices',
        api_version = 'auto',
        auto_remove = 'success',
        docker_url = 'tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS':'{{ti.xcom_pull(task_ids="store_prices")}}'
        }
    )

    get_formatted_csv = PythonOperator(
        task_id = 'get_formatted_csv',
        python_callable=get_formatted_csv,
        op_kwargs={
            'path' : '{{ti.xcom_pull(task_ids="store_prices")}}'
        }
    )

    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(
            path = f"s3://stock-market/{{{{ti.xcom_pull(task_ids='get_formatted_csv')}}}}",
            conn_id='minio'
        ),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        ),
        load_options={
            "aws_access_key_id": BaseHook.get_connection('minio').login,
            "aws_secret_access_key": BaseHook.get_connection('minio').password,
            "endpoint_url": BaseHook.get_connection('minio').host

        }
    )

is_api_available >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw













