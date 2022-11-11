import scrapy, json, os, hashlib, pymysql
from scrapy.cmdline import execute
from jahez.dbconfig import *
from jahez.items import JahezData

class DataSpider(scrapy.Spider):
    name = 'data'
    def start_requests(self):
        self.links = ConfigDatabase(table='links', database='jahez').fetchResultsfromSql()
        for i in self.links:
            url = i['URL']
            restaurant = i['Restaurant']
            yield scrapy.Request(url=url, meta={'url': url, 'restaurant': restaurant}, callback=self.parse)

    def parse(self, response):
        item = JahezData()
        jsonData = json.loads(response.text)

        item['Restaurant'] = response.meta['restaurant']
        item['URL'] = response.meta['url']
        
        for count, i in enumerate(jsonData['menuMobileList'], start=1):
            item[f'Item{count}_name'] = i['itemName']
            item[f'Item{count}_category'] = i['categoryNameEn']
            
            item[f'Item{count}_imageURL'] = i['imageUrl']
            if not item[f'Item{count}_imageURL']:
                item[f'Item{count}_imageURL'] = ''
            
            item[f'Item{count}_description'] = i['description']
            if not item[f'Item{count}_description']:
                item[f'Item{count}_description'] = ''

            item[f'Item{count}_price'] = i['prize']
            key = item['Restaurant'] + item['URL']
            item['_id'] = hashlib.md5(key.encode()).hexdigest()

        yield item
        
        path = "D:/Mohit_sharing/Jahez/"
        if not os.path.exists(path):
            os.makedirs(path)
        with open(f"{path}{item['_id']}.html", 'wb') as f:
            f.write(response.body)

        ConfigDatabase(table='links', database='jahez').updateStatusSql(item=item)
        print("Done")

if __name__ == '__main__':
    execute('scrapy crawl data'.split())