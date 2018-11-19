import scrapy
from scrapy import signals
from scrapy.loader import ItemLoader
from cgv_crawler.items import CgvCrawlerItem
from datetime import datetime, timedelta


locations = [
    '0001',  # 강변
    '0056',  # 강남
    '0229',  # 건대입구
    '0010',  # 구로
    '0063',  # 대학로
    '0252',  # 동대문
    '0009',  # 명동X正몰
    '0105',  # 명동역 씨네라이브러리
    '0105',  # 명동역 씨네라이브러리
]


class CgvCrawlSpider(scrapy.Spider):
    name = 'cgv_crawler'
    custom_settings = {
        'ITEM_PIPELINES': {
            'cgv_crawler.pipelines.JsonPipeline': 400
        }
    }
    allowed_domains = ['m.cgv.co.kr']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CgvCrawlSpider, cls).from_crawler(crawler, *args, **kwargs)
        spider.now = datetime.now()
        spider.start_dates = [spider.now, (spider.now + timedelta(1))]
        spider.locations = locations
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
            for li in link.xpath('.//ul[@class="timelist"]//li'):
                i = ItemLoader(item=CgvCrawlerItem(), response=response)
                i.add_value('th_location_id', response.request.meta['loca'])
                i.add_value('mv_title', link.xpath('./h3/text()').extract())
                i.add_value('mv_time', response.request.meta['date'].strftime("%Y-%m-%d")
                            + ' ' + li.xpath('.//text()').extract_first())
                yield i.load_item()
