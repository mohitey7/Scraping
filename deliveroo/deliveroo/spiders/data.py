import scrapy, json, pymysql, time
from deliveroo.items import DeliverooData

class DataSpider(scrapy.Spider):
    name = 'data'
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='deliveroo.ie')
    cursor = connection.cursor()
    headers = {
                    'authority': 'deliveroo.ie',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'accept-language': 'en-US,en;q=0.8',
                    'cache-control': 'max-age=0',
                    # Requests sorts cookies= alphabetically
                    'cookie': 'roo_guid=5908bf8a-39cf-4a1d-98d0-85ab37ec0ada; roo_guid=5908bf8a-39cf-4a1d-98d0-85ab37ec0ada; OptanonAlertBoxClosed=2022-08-30T11:48:12.491Z; location_data=eyJsb2NhdGlvbiI6eyJjb29yZGluYXRlcyI6Wy02LjI2MDMwOTcsNTMuMzQ5ODA1M10sImlkIjpudWxsLCJmb3JtYXR0ZWRfYWRkcmVzcyI6IkR1YmxpbiwgSXJlbGFuZCIsInBsYWNlX2lkIjoiQ2hJSkw2d242b0FPWjBnUm9IRXhsNm5IQUFvIiwicGluX3JlZmluZWQiOmZhbHNlLCJjaXR5IjpudWxsfX0.; roo_session_guid=1d6bac05-b07c-41f8-807a-530fa72d75b3; locale=eyJsb2NhbGUiOiJlbiJ9; OptanonConsent=isGpcEnabled=1&datestamp=Fri+Sep+02+2022+11%3A17%3A28+GMT%2B0530+(India+Standard+Time)&version=6.27.0&isIABGlobal=false&consentId=8c1b9a9f-0a2e-4e9c-85f0-a7d9ade4f6e0&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&hosts=H50%3A1%2CH37%3A1%2CH49%3A1%2CH24%3A1%2CH35%3A1%2CH38%3A1%2CH21%3A1%2CH39%3A1%2CH62%3A1%2CH4%3A1%2CH3%3A1%2CH22%3A1%2CH6%3A1%2CH25%3A1%2CH26%3A1%2CH20%3A1%2CH11%3A1%2CH10%3A1%2CH17%3A1%2CH19%3A1&genVendors=&geolocation=IN%3BGJ&AwaitingReconsent=false',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-user': '?1',
                    'sec-gpc': '1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36',
                }    

    def start_requests(self):
        DataSpider.cursor.execute("SELECT * FROM `dublin restaurants` where Status = 'Pending'")    
        restaurants = DataSpider.cursor.fetchall()

        for restaurant in restaurants:
            yield scrapy.Request(url=restaurant[1], headers=DataSpider.headers, callback=self.parse)
            
    def parse(self, response):
        if response.status != 200:
            time.sleep(2)
            yield scrapy.Request(url=response.url, headers=DataSpider.headers, callback=self.parse)

        item = DeliverooData()
            
        jsonData = json.loads(response.xpath("//*[@id='__NEXT_DATA__']/text()").get())
        
        # Name
        name = response.xpath('//h1[@class="ccl-cc80f737565f5a11 ccl-de2d30f2fc9eac3e ccl-05906e3f85528c85 ccl-483b12e41c465cc7"]/text()').get()
        name = name.strip()

        url = response.url

        # Address
        try:
            address = jsonData['props']['initialState']['menuPage']['menu']['meta']['restaurant']['location']['address']['address1']
            address = address.replace('"', '')
        except:
            address = "None"
        
        # Cuisines
        cuisines = response.xpath('//*[@class="UILines-24491e27014cf95d ccl-2d0aeb0c9725ce8b ccl-45f32b38c5feda86"]/span[@class="ccl-649204f2a8e630fd ccl-a396bc55704a9c8a ccl-0956b2f88e605eb8"]/text()').getall()
        if len(cuisines)>0:
            cuisines = ' | '.join([i.strip() for i in cuisines if (i!= '' and i!='.')]).strip()
        else:
            cuisines = "None"
        
        # Rating
        try:
            xpathData = response.xpath('//div[@class="UILines-24491e27014cf95d ccl-2d0aeb0c9725ce8b ccl-45f32b38c5feda86"]//text()').getall()
            for i in xpathData:
                if i[0].isdigit():
                    rating = i
                    break
            else:
                rating = "None"
        except:
            rating = "None"        

        # Description
        try:
            description = jsonData['props']['initialState']['menuPage']['menu']['meta']['metatags']['descriptionSocial']
            description = description[25:].strip().replace('"', '')
        except:
            description = "None"

        # Reviews
        try:
            for i in xpathData:
                if "(" in i[0]:
                    reviews = i.strip("()")
                    break
            else:
                reviews = "None"
        except:
            reviews = "None"

        # Delivery fee
        try:
            for i in xpathData:
                if "delivery" in i:
                    if "Free" not in i:
                        delivery_fee = i.strip(" delivery")
                    else:
                        delivery_fee = i.strip()
                    break
            else:
                delivery_fee = "None"
        except:
            delivery_fee = "None"

        # Contact_number
        try:
            contact_number = jsonData['props']['initialState']['menuPage']['menu']['layoutGroups'][1]['layouts'][0]['blocks'][0]['actions'][0]['target']['params'][0]['value'][0]
            contact_number = contact_number.strip()
        except:
            contact_number = "None"

        # Minimum order amount
        for i in xpathData:
            if "minimum" in i:
                minimum_order_amount = i.strip(" minimum")
                break
        else:
            minimum_order_amount = "None"
    

        item['name'] = name
        item['url'] = url
        item['address'] = address
        item['cuisines'] = cuisines
        item['rating'] = rating
        item['description'] = description
        item['reviews'] = reviews
        item['delivery_fee'] = delivery_fee
        item['minimum_order_amount'] = minimum_order_amount
        item['contact_number'] = contact_number
        
        yield item


# if __name__ == '__main__':
#     execute('scrapy crawl data'.split())

# if __name__ == '__main__':
#     import os
#     from scrapy.cmdline import execute

#     os.chdir(os.path.dirname(os.path.realpath(__file__)))

#     # SPIDER_NAME = data.name
#     try:
#         execute(
#             [
#                 'scrapy',
#                 'crawl',
#                 'data',
#                 '-s',
#                 'FEED_EXPORT_ENCODING=utf-8',
#             ]
#         )
#     except SystemExit:
#         pass