import os
from time import time

import pymongo
from scrapy import signals, Spider
from scrapy.responsetypes import responsetypes
from scrapy.utils.request import request_fingerprint
from scrapy.http import Headers
from scrapy.exceptions import NotConfigured


try:
    from pymongo import MongoClient, MongoReplicaSetClient
    from pymongo.errors import ConfigurationError
    from pymongo import version_tuple as mongo_version
    from gridfs import GridFS, errors
except ImportError:
    MongoClient = None


class MongoCacheStorage(object):
    """Storage backend for Scrapy HTTP cache, which stores responses in MongoDB
    GridFS.
    If HTTPCACHE_SHARDED is True, a different collection will be used for
    each spider, similar to FilesystemCacheStorage using folders per spider.
    """

    def __init__(self, settings, **kw):

        self.expire = settings.getint('HTTPCACHE_EXPIRATION_SECS')
        self.sharded = settings.getbool('HTTPCACHE_SHARDED', False)
        self.fs = {}

    def open_spider(self, spider: Spider):
        _shard = 'httpcache'
        if self.sharded:
            _shard = 'httpcache.%s' % spider.name
        self.fs[spider] = GridFS(spider.db, _shard)

    def close_spider(self, spider):
        del self.fs[spider]

    def retrieve_response(self, spider: Spider, request):
        key = self._request_key(spider, request)
        gf = self._get_file(spider, key)
        if gf is None:
            return None

        # request is cached
        spider.num_cached_request += 1
        url = str(gf.url)
        spider.logger.info("{} is cached".format(url))
        status = str(gf.status)
        header: dict = {key.encode('ascii'): list(map(lambda x: x.encode('ascii'), value))
                        for key, value in gf.headers.items()}
        headers = Headers(header)
        body = gf.read()
        respcls = responsetypes.from_args(headers=headers, url=url)
        response = respcls(url=url, headers=headers, status=status, body=body)
        return response

    def store_response(self, spider, request, response):
        key = self._request_key(spider, request)
        headers = {key.decode('ascii'): list(map(lambda x: x.decode('ascii'), value))
                   for key, value in dict(response.headers).items()}

        if response.status != 200:
            return

        metadata = {
            '_id': key,
            'time': time(),
            'status': response.status,
            'url': response.url,
            'headers': headers
        }

        if spider.name == 'chotot':
            if 'page' in response.url or 'robots' in response.url:
                return
        elif spider.name == 'homedy':
            if 'cho-thue-can-ho-ha-noi/p' in response.url or 'robots' in request.url:
                return
        elif spider.name == 'alonhadat':
            if '/nha-dat/cho-thue/can-ho-chung-cu/1/ha-noi/trang' in response.url or 'robots' in request.url:
                return

        try:
            self.fs[spider].put(response.body, **metadata)
        except errors.FileExists:
            self.fs[spider].delete(key)
            self.fs[spider].put(response.body, **metadata)

    def _get_file(self, spider, key):
        try:
            gf = self.fs[spider].get(key)
        except errors.NoFile:
            return None
        if 0 < self.expire < time() - gf.time:
            return None
        return gf

    @staticmethod
    def _request_key(spider, request):
        rfp = request_fingerprint(request)

        return '%s/%s' % (spider.name, rfp)