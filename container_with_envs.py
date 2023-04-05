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
import pandas

################################################################# Configuration #######################################################################

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/googleapi/level-approach-382012-1b97f11ea02f.json'
project_id_billing = 'smooth-league-382303'  # A Project where you have biquery.readsession permission
bqstorageclient = BigQueryReadClient()
project_id = "smooth-league-382303"

dataset_id = "gcpdataset"
table_id = "my-table-customer-records-2"
table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

read_options = ReadSession.TableReadOptions(selected_fields=["id", "name", "Time"])
# read_options.row_restriction = "partition_field like '%INSBI1%'"
# read_options.row_restriction = "partition_field BETWEEN 0 AND 1"
PARTITION_FIELD = os.environ.get('PARTITION_FIELD')
read_options.row_restriction = "partition_field = {}".format(PARTITION_FIELD)
parent = "projects/{}".format(project_id_billing)
requested_session = ReadSession(table=table, data_format=DataFormat.ARROW, read_options=read_options, )
read_session = bqstorageclient.create_read_session(parent=parent, read_session=requested_session, max_stream_count=1, )

# '''
consumer = KafkaConsumer('my-topic', bootstrap_servers=['35.225.83.11:9094'], auto_offset_reset='latest')

for message in consumer:
    producer = KafkaProducer(bootstrap_servers=['35.225.83.11:9094'], api_version=(0, 10))
    received = {"Received at: ": str(int(round(time.time())))}
    producer.send('my-second-topic', json.dumps(received).encode('utf-8'))
    stream = read_session.streams[0]  # read every stream from 0 to 3
    reader = bqstorageclient.read_rows(stream.name)
    # rows = reader.rows(read_session)
    x1 = message.value
    x2 = x1.decode('utf8')
    x3 = json.loads(x2)["sanction_payload"]
    # count = 0
    frames = []

    for my_message in reader.rows().pages:
        dict = {"customer_details_payload": my_message.to_dataframe().to_dict(), "sanction_payload": x3}
        producer.send('my-first-topic', json.dumps(dict).encode('utf-8'))

    if producer is not None:
        completed = {"Completed data-fetch and send at: ": str(int(round(time.time())))}
        producer.send('my-second-topic', json.dumps(completed).encode('utf-8'))
        producer.close()
# '''


'''
reader = bqstorageclient.read_rows(read_session.streams[0].name)
rows = reader.rows(read_session)

ids = set()
names = set()


count = 0

for row in reader.rows():
    ids.add(row["id"])
    names.add(row["name"])
    count = count + 1
    if count == 10:
        break

print(ids)
print(names)
'''
'''    
stream = read_session.streams[0] #read every stream from 0 to 3
reader = bqstorageclient.read_rows(stream.name)
print(read_session.streams)
count = 0
frames = []
for my_message in reader.rows().pages:
    frames.append(my_message.to_dataframe())
    if count==1:
        break
dataframe = pandas.concat(frames)
print(dataframe)



'''
