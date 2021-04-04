# smartsensor.py

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.filesystem import FSHook
from plugins.smart_file_sensor import SmartFileSensor

import logging
import json

default_args = {
    'start_date': days_ago(1),
}

@dag(schedule_interval='@daily', default_args=default_args, catchup=False)
def smartsensor():
    FILENAME = 'bitcoin.json'

    sensors = [
        SmartFileSensor(
            task_id=f'waiting_for_file_{sensor_id}',
            filepath=FILENAME,
            fs_conn_id='fs_default'
        ) for sensor_id in range(1, 10)
    ]

    @task
    def proccessing():
        path = FSHook(conn_id='fs_default').get_path()
        logging.info(f'{path}/{FILENAME}')

    @task
    def storing():
        logging.info('storing data')

    sensors >> proccessing() >> storing()

dag = smartsensor()