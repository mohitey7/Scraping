import scrapy, json, sys
sys.path.append('../shopsy')
from shopsy.dbconfig import ConfigSql
from shopsy.items import ShopsyData
from scrapy.utils.response import open_in_browser
from scrapy.cmdline import execute

class DataSpider(scrapy.Spider):
    name = 'data'
    links = ConfigSql(database='shopsy', table='links')

    headers = {
        'Cookie': 'SN=VI2B3214D371D840E6BD86B876648C4302.TOK4E38B091E6AC4211AF986D621F37F32A.1667466434.LO; T=cla0tu09xx7qe0901whvhkfusBR%3A.1667465378277'
        }

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def start_requests(self):
        start = int(self.start)
        end = int(self.end)

        for i in DataSpider.links.fetch(start, end):
            # yield scrapy.Request(url=i['URL'], callback=self.parse, meta={'url': i['URL']})
            yield scrapy.Request(url='https://www.shopsy.in/cubix-flip-cover-huawei-p30/p/itm6a56c14d9755a', callback=self.parse)

    def parse(self, response):
        item = ShopsyData()
        jsonData = response.xpath('//script[@id="is_script"]/text()').get()
        jsonData = jsonData.split('window.__INITIAL_STATE__ = ')[1]
        jsonData = json.loads(jsonData[:-1])

        item['ProductID'] = jsonData['multiWidgetState']['pageDataResponse']['pageContext']['productId']
        
        try:
            item['CategoryL1'] = jsonData['multiWidgetState']['pageDataResponse']['pageContext']['analyticsData']['category']
        except:
            item['CategoryL1'] = ''

        try:
            item['CategoryL2'] = jsonData['multiWidgetState']['pageDataResponse']['pageContext']['analyticsData']['subCategory']
        except:
            item['CategoryL2'] = ''

        item['URL'] = response.meta['url']

        yield item

if __name__ == '__main__':
    execute(f"scrapy crawl data -a start=1 -a end=10".split())