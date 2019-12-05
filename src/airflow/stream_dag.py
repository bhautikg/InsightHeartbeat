from airflow.operators import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import time
import sys
import os
import boto3
import pytz
from kafka.producer import KafkaProducer
import logging
sys.path.append('../python/')



args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 12, 4),
}

dag = DAG(
    dag_id='ecg_airflow', default_args=args,
    schedule_interval=timedelta(hours=0.5))


def my_function(file_key):
    '''This is a function that will run within the DAG execution'''
    producer = KafkaProducer(bootstrap_servers='10.0.0.24', linger_ms=4000)
    msg_cnt = 0
    tz = pytz.timezone('America/Los_Angeles')
    init_time= datetime.now(tz)
    fs=360
    while True:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket="bhaudata",
                            Key="101_signals.txt")
        for line in obj['Body'].iter_lines():
            message_info = None
            try:
                linesplit = line.decode()
                str_fmt = "{},{},{}"
                timestamp= init_time + timedelta(seconds=round((msg_cnt/fs), 3))
                y = timestamp.strftime("%H:%M:%S.%f")
                y = y[:-3]
                message_info = str_fmt.format(file_key,
                                              y,
                                              linesplit
                                              )
            except Exception as e:
                print("fixn problem")
            try:
                msg = str.encode(message_info)
            except Exception as e:
                msg = None
                #self.logger.debug('empty message %s'%e)
            if msg is not None:
                producer.send("ecg-topic2", msg)
                msg_cnt += 1
        break

task = PythonOperator(
        task_id='ecg_signal',
        python_callable=my_function,
        op_kwargs={'file_key': '101'},
        dag=dag)

