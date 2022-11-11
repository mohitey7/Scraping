import scrapy, json, sys, os
sys.path.append('../mamaearth')
from mamaearth.items import MamaearthData
from mamaearth.dbconfig import ConfigSql
from scraper_helper import run_spider


class DataSpider(scrapy.Spider):
    name = 'data'
    links = ConfigSql(database='mamaearth', table='links')

    def start_requests(self):
        for i in DataSpider.links.fetch():
            url = i['URL']
            yield scrapy.Request(url=url, callback=self.parse, meta={'url': url})
    
    def parse(self, response):
        item = MamaearthData()

        jsonData = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        jsonData = json.loads(jsonData)
        
        item['URL'] = response.meta['url']
        item['Product'] = jsonData['props']['initialState']['fetchedProduct']['data']['item']['name']
        item['SKU'] = jsonData['props']['initialState']['fetchedProduct']['data']['item']['sku']

        item['MRP'] = jsonData['props']['initialState']['fetchedProduct']['data']['item']['price']
        if item['MRP'] and '.' in item['MRP']:
            item['MRP'] = item['MRP'].split('.')[0].replace(',', '')

        item['Quantity'] = jsonData['props']['initialState']['fetchedProduct']['data']['item']['qty']
        item['ReviewCount'] = jsonData['props']['initialState']['fetchedProduct']['data']['item']['review_count']
        item['AverageRatingPercent'] = jsonData['props']['initialState']['fetchedProduct']['data']['item']['avg_rating_percent']
        item['CreatedAt'] = jsonData['props']['initialState']['fetchedProduct']['data']['item']['createdAt']
        item['UpdatedAt'] = jsonData['props']['initialState']['fetchedProduct']['data']['item']['updatedAt']
        
        item['SoldOut'] = jsonData['props']['initialState']['fetchedProduct']['data']['item']['is_in_stock']
        if item['SoldOut'] == 0:
            item['SoldOut'] = 'Yes'
        else:
            item['SoldOut'] = 'No'

        rating = response.xpath('//a[@class="rating"]/text()').getall()
        if rating:
            for i in rating:
                if i[0].isnumeric():
                    item['Rating'] = i
        
        item['Category'] = response.xpath('//div[contains(@class, "ProductDetails")]//a[contains(@href, "product-category")]/text()').getall()
        if item['Category']:
            item['Category'] = ' | '.join(item['Category'])
            if '-' in item['Category']:
                item['Category'] = item['Category'].split('-')
                item['Category'] = '-'.join([x.capitalize() for x in item['Category']])
            else:
                item['Category'] = item['Category'].capitalize()
        else:
            item['Category'] = ''
        
        item['Images'] = jsonData['props']['initialState']['fetchedProduct']['data']['item']['images']
        if item['Images']:
            item['Images'] = ' | '.join(item['Images'])

        item['Subtitle'] = response.xpath('//div[@class="ProductDetailsRevamp__Wrapper-sc-1w9tx2u-0 eNdnDW"]/div[@class="subtitle"]/text()').get()
        if not item['Subtitle']:
            item['Subtitle'] = ''
        
        jsonData = response.xpath('//script[@type="application/ld+json"]/text()').get()
        jsonData = json.loads(jsonData)

        item['Price'] = jsonData['offers']['price']

        path = f'{os.getcwd()}\\PageData\\'
        if not os.path.exists(path):
            os.makedirs(path)
        with open(f"{path}{item['SKU']}.html", 'wb') as f:
            f.write(response.body)

        yield item

if __name__ == '__main__':
    run_spider(DataSpider)