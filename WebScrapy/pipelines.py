import json
import requests
import dataclasses
from typing import List
from datetime import datetime

from bs4 import BeautifulSoup
from unidecode import unidecode
from scrapy import Spider
from WebScrapy.items import ChoTotRawNewsItem, HomedyRawNewsItem, AlonhadatRawNewsItem, BatDongSanRawNewsItem, \
    BatDongSanNewsItem
from WebScrapy.items import ChototNewsItem, HomedyNewsItem, AlonhadatNewsItem
from WebScrapy.utils import process_upload_time, timedelta


class AlonhadatPipeline:
    def process_item(self, item: AlonhadatNewsItem, spider: Spider):

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('Message delivery failed: {}'.format(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

        spider.producer.poll(0)
        spider.producer.produce(spider.topic,
                                json.dumps(dataclasses.asdict(item)).encode('utf-8'),
                                callback=delivery_report)
        spider.producer.flush()

