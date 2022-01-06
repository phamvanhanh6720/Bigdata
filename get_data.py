import json
import base64
import hashlib
from json import dump

import pandas as pd
from confluent_kafka import Consumer
from hdfs import InsecureClient

c = Consumer({'bootstrap.servers': '127.0.0.1:9092',
              'group.id': 'bigdatagroup',
              'auto.offset.reset': 'earliest'})
c.subscribe(['crawled_news'])
client_hdfs = InsecureClient('http://localhost:9870', user='root')

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    data_dict = json.loads(msg.value().decode('utf-8'))
    """
    columns = sorted(list(data_dict.keys()))
    for key in data_dict.keys():
        data_dict[key] = [data_dict[key]]
    df = pd.DataFrame.from_dict(data_dict)
    """
    try:
        url = str(data_dict['url'])
        hash_url = hashlib.md5(url.encode()).hexdigest()

        with client_hdfs.write('/new_alonhadatnews_json/{}.jsonl'.format(hash_url), encoding='utf-8') as writer:
            dump(data_dict, writer)
        print('Write {} to Hdfs successfully'.format(data_dict['url']))
    except:
        print('{} existed in hdfs'.format(data_dict['url']))

c.close()