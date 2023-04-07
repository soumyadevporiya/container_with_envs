'''Now here we leverage power of both multiprocessing and multithreading. Created 10 parallel processes and each
process has 5 threads. 2.1 mil obs are processed in 6 seconds.

10 threads per process do not improve speed

'''
import requests
import time
import json
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage
import os
from kafka import KafkaConsumer
from kafka import KafkaProducer
import urllib.request
from google.cloud.bigquery_storage import ReadSession
from google.cloud.bigquery_storage import DataFormat
import pandas as pd

from multiprocessing import Process, Queue, Pool
import multiprocessing as mp
from threading import Thread


def thread_task(q: Queue):
    while True:
        payload = q.get()
        if type(payload) == dict:
            url_post = 'http://34.67.224.29:80/hello/post'  # 'http://192.168.101.163:80/hello/post'  #
            post_response = requests.post(url_post, data=payload)
            x = post_response.text  # side_input
            print(x)
        else:
            break


def process_task(q: Queue):
    from kafka import KafkaProducer
    packets_processed = 0

    NUMBER_OF_THREADS = 5

    list_of_threads = []
    list_of_thread_qs = []
    for i in range(NUMBER_OF_THREADS):
        list_of_thread_qs.append(Queue())
    for i in range(NUMBER_OF_THREADS):
        list_of_threads.append(Thread(target=thread_task, args=(list_of_thread_qs[i],)))
    for i in range(NUMBER_OF_THREADS):
        list_of_threads[i].start()

    counter_process = 0

    while True:
        payload = q.get()
        if type(payload) == dict:
            counter_process = counter_process + 1
            modulus_local = counter_process % NUMBER_OF_THREADS
            list_of_thread_qs[modulus_local].put(payload)
        else:
            for i in range(NUMBER_OF_THREADS):
                list_of_thread_qs[i].put("Reading has ended, Please Come Out")

            for i in range(NUMBER_OF_THREADS):
                list_of_threads[i].join()
            break


if __name__ == '__main__':

    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/googleapi/smooth-league-382303-bb2d5d81cbed.json'

    project_id_billing = 'smooth-league-382303'  # A Project where you have biquery.readsession permission

    bqstorageclient = BigQueryReadClient()

    project_id = "smooth-league-382303"

    dataset_id = "gcpdataset"
    table_id = "my-table-customer-records-2"
    table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    read_options = ReadSession.TableReadOptions(
        selected_fields=["id", "name"]
    )
    # read_options.row_restriction = "partition_field like '%INSBI1%'"
    #read_options.row_restriction = "partition_field = 1"
    PARTITION_FIELD = os.environ.get('PARTITION_FIELD')
    read_options.row_restriction = "partition_field = {}".format(PARTITION_FIELD)
    parent = "projects/{}".format(project_id_billing)

    requested_session = ReadSession(
        table=table,
        data_format=DataFormat.ARROW,
        read_options=read_options,
    )

    read_session = bqstorageclient.create_read_session(
        parent=parent,
        read_session=requested_session,
        max_stream_count=1,
    )

    consumer = KafkaConsumer('my-topic', bootstrap_servers=['35.225.83.11:9094'], auto_offset_reset='latest')

    list_of_process_qs = []
    list_of_process = []

    NUMBER_OF_PROCESSES = 10

    for i in range(NUMBER_OF_PROCESSES):
        list_of_process_qs.append(Queue())
        list_of_process.append(Process(target=process_task, args=(list_of_process_qs[i],)))
        list_of_process[i].start()

    for message in consumer:
        producer = KafkaProducer(bootstrap_servers=['35.225.83.11:9094'], api_version=(0, 10))
        received = {"Received at: ": str(int(round(time.time())))}
        producer.send('my-second-topic', json.dumps(received).encode('utf-8'))
        stream = read_session.streams[0]  # read every stream from 0 to 3
        reader = bqstorageclient.read_rows(stream.name)

        x1 = message.value
        x2 = x1.decode('utf8')
        x3 = json.loads(x2)["sanction_payload"]

        frames = []
        counter = 0

        for my_message in reader.rows().pages:
            # dict = {"customer_details_payload": my_message.to_dataframe().to_dict(),"sanction_payload":x3}
            # producer.send('my-first-topic', json.dumps(dict).encode('utf-8'))
            data_dict = {
                'customer_id': my_message.to_dataframe().to_dict()['id'],
                'customer_name': my_message.to_dataframe().to_dict()['name'],
                'sanctioned_name': x3.split(',')
            }

            counter = counter + 1
            modulus = counter % NUMBER_OF_PROCESSES
            list_of_process_qs[modulus].put(data_dict)

        if producer is not None:
            producer.close()

        for i in range(NUMBER_OF_PROCESSES):
            list_of_process_qs[i].put("Reading has ended, Please Come Out")

        for i in range(NUMBER_OF_PROCESSES):
            list_of_process[i].join()

        print("Experiment Ended")
