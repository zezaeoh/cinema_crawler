from scrapy import signals
from selenium import webdriver
from scrapy.http import HtmlResponse
from selenium.common.exceptions import TimeoutException

import time


class JavaScriptMiddleware(object):
    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        s.driver = webdriver.PhantomJS()
        s.driver.set_page_load_timeout(10)
        s.retry = 0
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s

    def process_request(self, request, spider):
        print("rendering...")
        try:
            self.driver.get(request.url)
        except TimeoutException:
            print('time out! rendering restart')
            return request
        time.sleep(1)
        body = self.driver.page_source.encode('utf-8')
        print("parsing... " + request.url)
        rp = HtmlResponse(self.driver.current_url, body=body, encoding='utf-8', request=request)
        return rp

    def process_response(self, request, response, spider):
        return response

    def spider_closed(self, spider):
        self.driver.quit()
        print("driver closed!")
