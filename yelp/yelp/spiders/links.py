import scrapy
from ..items import YelpItem

class LinksSpider(scrapy.Spider):
    name = 'links'

    def start_requests(self):
        url = 'https://www.yelp.co.uk/search?find_desc=Digital+Marketing+Jobs&find_loc=united+kingdom'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        item = YelpItem()

        companies = list(set(response.xpath('//span[@class=" css-1egxyvc"]/a/@href').getall()))
        for i in range(len(companies)):
            companies[i] = 'https://www.yelp.co.uk' + companies[i].split('?')[0]
            item['company'] = companies[i]
            yield item
        
        next_page = response.xpath('//span[@class=" css-foyide"]/a[@class="next-link navigation-button__09f24__m9qRz css-144i0wq"]/@href').get()
        
        if next_page is not None:
            yield scrapy.Request(url=next_page, callback=self.parse)