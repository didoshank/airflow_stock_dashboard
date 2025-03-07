from airflow.decorators import dag,task
from datetime import datetime
import random

@dag(
    start_date = datetime(2025,1,1),
    schedule_interval='@daily',
    catchup=False,
    description='A simple dag to generate and check random numbers',
    tags=['taskflow']
)


def taskflow():
    
    @task
    def task_a():
       number = random.randint(1,100)
       return number
    
    @task
    def task_b(value):
        if(value%2==0):
            return "even"
        return "odd"
    
    task_b(task_a())


taskflow()