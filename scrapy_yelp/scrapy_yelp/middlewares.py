# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import json
import logging
import requests
import multiprocessing
from requests.exceptions import ConnectionError

class ProxyMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.
    def __init__(self,proxy_pool_url):
        self.logger = logging.getLogger(__name__)
        self.proxy_pool_url = proxy_pool_url

    def _get_proxy(self):
        try:
            # 获取localhost中的代理
            response = requests.get(self.proxy_pool_url,allow_redirects=False,)
            if response.status_code == 200:
                return response.text
        except ConnectionError:
            return None

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        return cls(
            proxy_pool_url = crawler.settings.get('PROXY_POOL_URL')
        )


    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_requests(self, request, spider):
        proxy = self._get_proxy()
        self.logger.debug("process_requests get proxy",proxy)
        if proxy:
            print("this is ip:" + str(proxy))
            request.meta["proxy"] = "http://" + str(proxy)
        else:
            self.logger.debug("No valid Proxy")

    def process_response(self,request,response,spider):
        if response.status != 200:
            print('ip forbidden')
            request.meta["proxy"] = "http://" + self._get_proxy()
            #self.logger.debug("获取代理",daili)
            return request
        else:
            return response


    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
