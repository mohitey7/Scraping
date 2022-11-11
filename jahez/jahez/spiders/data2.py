import scrapy, json, hashlib
from jahez.dbconfig import *
from jahez.items import JahezData

class Data2Spider(scrapy.Spider):
    name = 'data2'
    
    def start_requests(self):
        links = ConfigDatabase(table='links', database='jahez').fetchResultsfromSql()
        for i in links:
            url = i['URL']
            restaurant = i['Restaurant']
            yield scrapy.Request(url=url, meta={'url': url, 'restaurant': restaurant}, callback=self.parse)

    def parse(self, response):
        item = JahezData()
        jsonData = json.loads(response.text)

        item['Restaurant'] = response.meta['restaurant']
        item['URL'] = response.meta['url']

        try:
            for i in jsonData['menuMobileList']:
                item['Item'] = i['itemName']
                item['Category'] = i['categoryNameEn']
                
                item['ImageURL'] = i['imageUrl']
                if not item['ImageURL']:
                    item['ImageURL'] = ''
                
                item['Description'] = i['description']
                if not item['Description']:
                    item['Description'] = ''

                item['Price'] = i['prize']
                
                key = item['Item'] + item['Category'] + item['Restaurant'] + item['URL']
                item['Hash'] = hashlib.md5(key.encode()).hexdigest()

                yield item
                # print(item)
            
            links = ConfigDatabase(table='links', database='jahez')
            links.updateStatusSql(item=item)

        except Exception as e:
            print(f'{e} in data insertion!')