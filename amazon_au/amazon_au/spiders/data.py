import scrapy, pymysql
from ..items import AmazonAuData
from ..dbconfig import *
from datetime import datetime

class DataSpider(scrapy.Spider):
    name = 'data'
    headers = {
            'authority': 'www.amazon.com.au',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-IN,en;q=0.8',
            'cache-control': 'max-age=0',
            # Requests sorts cookies= alphabetically
            # 'cookie': 'session-id=357-3121646-9551328; i18n-prefs=AUD; ubid-acbau=355-0388732-8322043; session-token="zdolGY28aMrSsQJdRoyh5oX9oc0Pvm3nJzWkkvsXy29mauRQylFYJyG0oPjDOvCTq6EPSqPxvbLITHW0kpuMzNdrK4IFdtTictAjod4MntlDMeRLmch7kvtmOQ6C3wIJd3r2vOYS3jwbYKAwbYWIMhvAeUTts3Y85cv8mpLDswBGCU3vyi/63LKcpF7eRraGZBJJDUXZ9OCN3400j1u+tqQG9BvlaePKCGEzQ7EddM0="; session-id-time=2082758401l; csm-hit=tb:H7SE74GJJTREJD0TMAAH+s-N8761N640V4A13V1MWQX|1664711484431&t:1664711484431&adb:adblk_no',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'sec-gpc': '1',
            'service-worker-navigation-preload': 'true',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
            }
    
    def __init__(self):
        self.connection = ConfigSql(database='amazonau', table='links')
    
    def start_requests(self):
        links = self.connection.fetch()
        for link in links:
            yield scrapy.Request(url=link['URL'], headers=DataSpider.headers, meta={'url':link['URL']}, callback=self.parse)

    def parse(self, response):
        item = AmazonAuData()

        item['URL'] = response.meta['url']
        
        item['Product'] = response.xpath('//*[@id="productTitle"]/text()').get()
        if item['Product']:
            item['Product'] = item['Product'].strip()
        
        item['Ratings'] = response.xpath('//*[@id="acrPopover"]/@title').get()
        if item['Ratings']:
            item['Ratings'] = item['Ratings'].split(' ')[0]
        else:
            item['Ratings'] = 0
        
        item['Reviews'] = response.xpath('//*[@id="acrCustomerReviewText"]/text()').get()
        if item['Reviews']:
            item['Reviews'] = item['Reviews'].split(' ')[0].replace(',', '')
        else:
            item['Reviews'] = 0
        
        item['Price'] = response.xpath('//*[@id="twister-plus-price-data-price"]/@value').get()
        if item['Price'] is None:
            item['Price'] = 'N/A'
        
        item['Brand'] = response.xpath('//*[@id="bylineInfo"]/text()').get()
        if item['Brand']:
            item['Brand'] = item['Brand'].split(': ')[1]
        else:
            item['Brand'] = 'N/A'

        if response.xpath('//span[@class="a-price-whole"]/text()').get() is None:
            item['Availability'] = 'Sold Out'
            item['Price'] = 'N/A'
        else:
            item['Availability'] = 'In Stock'

        item['Scraped_at'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
        yield item