import scrapy
from ..items import HarrisfarmsUrls

class UrlsSpider(scrapy.Spider):
    name = 'urls'

    def start_requests(self):
        url = 'http://www.harrisfarm.com.au/'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        
        categories = list(set(response.xpath('//a[contains(@href, "collection")]/@href').getall()))

        for i in range(len(categories)):
            if categories[i].startswith('/'):
                categories[i] = 'https://www.harrisfarm.com.au' + categories[i].strip()

        categories = list(set(categories))

        for category in categories:
            yield scrapy.Request(url=category, callback=self.parse2)

    def parse2(self, response):
        item = HarrisfarmsUrls()
        products = response.xpath('//p[@class="title"]/a/@href').getall()

        for i in products:
            item['link'] = "https://www.harrisfarm.com.au" + i.strip()
            yield item