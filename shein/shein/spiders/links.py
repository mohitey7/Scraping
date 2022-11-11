import scrapy
from ..items import SheinLinks

class LinksSpider(scrapy.Spider):
    name = 'links'
    start_urls = ['https://sg.shein.com/sitemap-index.xml']

    def parse(self, response):
        pageUrls = response.xpath("//*[contains(text(),'-products-')]/text()").getall()

        for url in pageUrls:
            yield scrapy.Request(url=url, callback=self.parse2)
    
    def parse2(self, response):
        item = SheinLinks()
        productUrls = response.xpath("//*[contains(text(),'.html')]/text()").getall()

        for url in productUrls:
            item['url'] = url
            yield item