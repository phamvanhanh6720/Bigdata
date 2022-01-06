from typing import List

import scrapy
import pymongo
from scrapy.crawler import CrawlerProcess

from confluent_kafka import Producer
from scrapy.utils.project import get_project_settings
from WebScrapy.items import AlonhadatItem

import logging
from selenium.webdriver.remote.remote_connection import LOGGER
from urllib3.connectionpool import log


log.setLevel(logging.WARNING)
LOGGER.setLevel(logging.WARNING)


class AlonhadatSpider(scrapy.Spider):
    name = 'alonhadat'
    allowed_domains = ['alonhadat.com.vn']
    custom_settings = {
        'HTTPCACHE_EXPIRATION_SECS': 43200,
        'MAX_CACHED_REQUEST': 4000,
        'ITEM_PIPELINES': {
            'WebScrapy.pipelines.AlonhadatPipeline': 300
        },
        'HTTPCACHE_STORAGE': 'WebScrapy.extensions.MongoCacheStorage'
    }

    def __init__(self):
        super(AlonhadatSpider, self).__init__()
        self.cfg = dict(get_project_settings())
        self.mongo_db = {
            'HOSTNAME': '127.0.0.1:27017',
            'USERNAME': 'webscrapy',
            'PASSWORD': '68f539388f66a374908f3df559eb4ea2',
            'DATABASE': 'realestate'}

        self.num_cached_request = 0
        # self.topic = self.cfg['KAFKA_TOPIC']
        self.topic = 'crawled_news'
        self.kafka = {'bootstrap.servers': '127.0.0.1:9092'}

        with open('WebScrapy/urls/start_urls_hanoi.txt', 'r') as file:
            start_urls = file.readlines()
            self.start_urls = [url.strip() for url in start_urls if url != '']
            print("Total start urls: {}".format(len(start_urls)))

        try:
            self.connection = pymongo.MongoClient(host=self.mongo_db['HOSTNAME'],
                                                  username=self.mongo_db['USERNAME'],
                                                  password=self.mongo_db['PASSWORD'],
                                                  authSource=self.mongo_db['DATABASE'],
                                                  authMechanism='SCRAM-SHA-1')
            self.db = self.connection[self.mongo_db['DATABASE']]
            self.logger.info("Connect database successfully")
        except:
            self.logger.error("Connect database unsuccessfully")
            self.__del__()

        try:
            self.producer = Producer(**self.kafka)
        except:
            self.logger.error('Connect to kafka fail')

    def __del__(self):
        self.logger.info("Close connection to database")
        self.connection.close()

    def parse(self, response):
        news_url_list: List[str] = response.css('div#content-body div#left div.content-item div.ct_title a::attr(href)').getall()

        page = response.request.url
        current_page = 1

        if 'trang' in page:
            current_page = int(page.split('--')[-1].replace('.html', ''))
            realestate_type = page.split('/')[-5]
            province = page.split('/')[-4]
            district = page.split('/')[-2]
            page = page.split('--')[0]
            next_page = page + '--{}.html'.format(current_page + 1)
        else:
            next_page = page.replace('.html', '') + '/trang--{}.html'.format(2)
            realestate_type = page.split('/')[-4]
            province = page.split('/')[-3]
            district = page.split('/')[-1].replace('.html', '')

        if len(news_url_list):

            for i in range(len(news_url_list)):
                news_url = news_url_list[i]
                news_url: str = response.urljoin(news_url)

                item_request = scrapy.Request(url=news_url,
                                              callback=self.parse_info,
                                              cb_kwargs={'realestate_type': realestate_type,
                                                         'province': province,
                                                         'district': district})

                yield item_request

            max_cached_request = 4000

            # if self.num_cached_request <= max_cached_request:
            if current_page <= 60:
                req = scrapy.Request(url=next_page, callback=self.parse)
                self.logger.info("Trying to follow link '{}'".format(req.url))

                yield req

    def parse_info(self, response, **kwargs):
        self.logger.info("Item url {}".format(response.request.url))

        # capture raw response

        # detail info
        realestate_type = kwargs['realestate_type']
        province = kwargs['province']
        district = kwargs['district']

        item = AlonhadatItem(realestate_type=realestate_type,
                             url=response.request.url,
                             province=province,
                             district=district,
                             status_code=response.status,
                             body=response.body,
                             encoding=response.encoding)

        # raw response has been processed, yield to item pipeline
        yield item


if __name__ == '__main__':
    setting = get_project_settings()
    process = CrawlerProcess(get_project_settings())
    process.crawl(AlonhadatSpider)
    process.start()