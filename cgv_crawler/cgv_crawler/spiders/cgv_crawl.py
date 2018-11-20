import scrapy
from scrapy import signals
from scrapy.loader import ItemLoader
from cgv_crawler.items import CgvCrawlerItem
from datetime import datetime, timedelta


class CgvCrawlSpider(scrapy.Spider):
    name = 'cgv_crawler'
    custom_settings = {
        'ITEM_PIPELINES': {
            'cgv_crawler.pipelines.MySqlPipeline': 400
        }
    }
    allowed_domains = ['m.cgv.co.kr']

    @classmethod
    def from_crawler(cls, crawler, init_table=False, **kwargs):
        spider = super(CgvCrawlSpider, cls).from_crawler(crawler, **kwargs)
        spider.now = datetime.now()
        spider.init_table = init_table
        spider.start_dates = [spider.now, (spider.now + timedelta(1))]
        spider.prefix = 'http://m.cgv.co.kr/'
        spider.postPage = 'http://m.cgv.co.kr/Schedule/?tc={}&t=T&ymd={}'
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        print('Work time:', datetime.now() - spider.now)

    def start_requests(self):
        for l in self.locations:
            for d in self.start_dates:
                url = self.postPage.format(l, d.strftime("%Y%m%d"))
                rq = scrapy.Request(url, callback=self.parse)
                rq.meta['date'] = d
                rq.meta['loca'] = l
                yield rq

    def parse(self, response):
        for link in response.xpath('//*[@id="divContent"]//section'):
            title = link.xpath('./h3/text()').extract()
            for li in link.xpath('.//ul[@class="timelist"]//li'):
                i = ItemLoader(item=CgvCrawlerItem(), response=response)
                i.add_value('th_location_id', response.request.meta['loca'])
                i.add_value('mv_title', title)
                i.add_value('mv_time', response.request.meta['date'].strftime("%Y-%m-%d")
                            + ' ' + li.xpath('.//text()').extract_first())
                yield i.load_item()
