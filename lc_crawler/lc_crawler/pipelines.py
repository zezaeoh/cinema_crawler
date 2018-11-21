from __future__ import unicode_literals
from scrapy.conf import settings
from scrapy.exporters import JsonItemExporter
from scrapy.exceptions import CloseSpider

import pymysql.cursors
import sys


# Mysql로 보내는 클래스
class MySqlPipeline(object):
    def __init__(self):
        try:
            self.conn = pymysql.connect(
                host=settings['MYSQL_HOST'],
                user=settings['MYSQL_USER'],
                password=settings['MYSQL_PASSWORD'],
                db=settings['MYSQL_DB'],
                charset=settings['MYSQL_CHARSET']
            )
        except Exception as e:
            print(str(e), file=sys.stderr)
            raise CloseSpider("ERROR: Unexpected error: Could not connect to MySql instance.")
        else:
            self.th_id = settings['TH_ID']
            self.cursor = self.conn.cursor()
            self.sql = 'insert into th_mv_times(th_id, br_id, mv_title, mv_time) values (%s, %s, %s, %s)'

    def open_spider(self, spider):
        spider.locations = []
        branch_sql = 'select br_id, br_div_code from th_branch where th_id = %s'
        self.cursor.execute(branch_sql, self.th_id)
        for a in self.cursor.fetchall():
            spider.locations.append([a[0] ,'%04d' % int(a[1])])
        if spider.init_table:
            init_sql = 'truncate th_mv_times'
            self.cursor.execute(init_sql)
            self.conn.commit()

    def process_item(self, item, spider):
        item['th_id'] = self.th_id
        self.cursor.execute(self.sql, [item['th_id'], item['th_location_id'], item['mv_title'], item['mv_time']])
        return item
    
    def close_spider(self, spider):
        self.cursor.close()
        self.conn.commit()
        self.conn.close()
        print('mysql connection over')


# JSON파일로 저장하는 클래스 (test)
class JsonPipeline(object):
    def __init__(self):
        self.file = open("lc_test.json", 'wb')
        self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item
