import scrapy, pymysql
from ..items import HarrisfarmsData

class DataSpider(scrapy.Spider):
    name = 'data'
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='harrisfarms')
    cursor = connection.cursor()

    def start_requests(self):
        DataSpider.cursor.execute("select * from links where Status = 'Pending'")
        links = DataSpider.cursor.fetchall()

        for link in links:
            yield scrapy.Request(url=link[1], callback=self.parse)

    def parse(self, response):
        item = HarrisfarmsData()

        description = response.xpath("//div[@itemprop='description']//text()").getall()

        for i in range(len(description)):
            description[i] = description[i].strip()

        description = ' | '.join([i for i in description if (i!= '' and i!='.' and not i.startswith('-'))])
        

        images = response.xpath('//div[@class="photos"]//@href').getall()
        for i in range(len(images)):
            images[i] = "https:" + images[i].strip()

        item['link'] = response.url
        item['title'] = response.xpath('//h1[@class="page-title"]/text()').get()
        item['description'] = description.replace('"', '')
        item['images'] = ' | '.join(images)

        if item['title'] is not None:
            yield item