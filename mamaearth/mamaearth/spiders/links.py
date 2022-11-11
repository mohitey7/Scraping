import scrapy, json, sys
# sys.path.append(r'C:\Users\Administrator\Desktop\Project\mamaearth\mamaearth')
sys.path.append('../mamaearth')
from mamaearth.items import MamaearthLinks
from scraper_helper import run_spider


class LinksSpider(scrapy.Spider):
    name = 'links'
    start_urls = ['https://mmrth-nd-api.honasa-production.net/v1/products/shopAllProducts']

    def parse(self, response):
        item = MamaearthLinks()
        
        jsonData = json.loads(response.text)
        links = jsonData['list']['result']
        
        for link in links:
            item['URL'] = 'https://mamaearth.in/product/' + link
            yield item

if __name__ == '__main__':
    run_spider(LinksSpider)