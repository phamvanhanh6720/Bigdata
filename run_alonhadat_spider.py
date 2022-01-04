from WebScrapy import AlonhadatSpider
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

import logging
from selenium.webdriver.remote.remote_connection import LOGGER
from urllib3.connectionpool import log


log.setLevel(logging.WARNING)
LOGGER.setLevel(logging.WARNING)


if __name__ == '__main__':
    setting = get_project_settings()
    process = CrawlerProcess(get_project_settings())
    process.crawl(AlonhadatSpider)
    process.start()