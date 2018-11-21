import scrapy
from scrapy import signals
from scrapy.loader import ItemLoader
from lc_crawler.items import LcCrawlerItem
from datetime import datetime


class LcCrawlSpider(scrapy.Spider):
    name = 'lc_crawler'
    custom_settings = {
        'ITEM_PIPELINES': {
            'lc_crawler.pipelines.MySqlPipeline': 400
        }
    }
    allowed_domains = ['www.lottecinema.co.kr']

    @classmethod
    def from_crawler(cls, crawler, init_table=False, **kwargs):
        spider = super(LcCrawlSpider, cls).from_crawler(crawler, **kwargs)
        spider.now = datetime.now()
        spider.init_table = init_table
        spider.prefix = 'http://www.lottecinema.co.kr/'
        spider.postPage = 'http://www.lottecinema.co.kr/LCMW/Contents/Cinema/cinema-detail.aspx?divisionCode=1&detailDivisionCode={}&cinemaID={}'
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        print('Work time:', datetime.now() - spider.now)

    def start_requests(self):
        for l in self.locations:
            url = self.postPage.format(l[1], l[0])
            rq = scrapy.Request(url, callback=self.parse)
            rq.meta['loca'] = l[0]
            yield rq

    def parse(self, response):
        for link in response.xpath('//*[@id="divTicketArea"]//div[@class="time_box time%s"]//dl' % response.request.meta['loca']):
            title = link.xpath('./dt//span[@class="tit_movie"]/text()').extract()
            for li in link.xpath('.//dd//span[@class="time"]'):
                i = ItemLoader(item=LcCrawlerItem(), response=response)
                i.add_value('th_location_id', response.request.meta['loca'])
                i.add_value('mv_title', title)
                i.add_value('mv_time', self.now.strftime("%Y-%m-%d")
                            + ' ' + li.xpath('./strong[@class="time_begin"]/text()').extract_first())
                yield i.load_item()
