import json
import logging
import os
import time
import uuid
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')
STREAM_DURATION_SECONDS = int(os.getenv('STREAM_DURATION_SECONDS', '60'))
STREAM_POLL_INTERVAL_SECONDS = float(os.getenv('STREAM_POLL_INTERVAL_SECONDS', '1'))

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}


def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/", timeout=10)
    res.raise_for_status()
    res = res.json()
    res = res['results'][0]

    return res


def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = str(location['postcode'])
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        max_block_ms=5000,
        value_serializer=lambda value: json.dumps(value).encode('utf-8')
    )
    deadline = time.monotonic() + STREAM_DURATION_SECONDS

    while time.monotonic() < deadline:
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', value=res)
            producer.flush()
            time.sleep(STREAM_POLL_INTERVAL_SECONDS)
        except Exception as e:
            logging.error(f'An error occured: {e}')
            time.sleep(STREAM_POLL_INTERVAL_SECONDS)
            continue

    producer.close()

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
