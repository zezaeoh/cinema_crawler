import scrapy
from scrapy.loader.processors import MapCompose, Join, TakeFirst


def filter_strip(v):
    return v.strip()

class LcCrawlerItem(scrapy.Item):
    th_id = scrapy.Field(
        output_processor=TakeFirst()
    )
    th_location_id = scrapy.Field(
        output_processor=TakeFirst()
    )
    mv_title = scrapy.Field(
        input_processor=MapCompose(filter_strip),
        output_processor=Join()
    )
    mv_time = scrapy.Field(
        output_processor=TakeFirst()
    )
    pass
