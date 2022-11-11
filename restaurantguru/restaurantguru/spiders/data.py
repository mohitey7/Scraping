import scrapy, pymysql, json
from ..items import RestaurantguruData

class DataSpider(scrapy.Spider):
    name = 'data'
    
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='restaurantguru')
    cursor = connection.cursor()

    headers = {
        'accept': ' text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'accept-encoding': ' gzip, deflate, br',
        'accept-language': ' en-US,en;q=0.9',
        'sec-fetch-dest': ' document',
        'sec-fetch-mode': ' navigate',
        'sec-fetch-site': ' none',
        'sec-fetch-user': ' ?1',
        'sec-gpc': ' 1',
        'upgrade-insecure-requests': ' 1',
        'user-agent': ' Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        # 'Cookie': 'PHPSESSID=e0938659e677a0519140ba15d98e3e38; currentCity=98641; currentCountry=73; dc_location=co73; pier_tutor_nums=%7B%22nums%22%3A%5B2%2C3%2C1%2C5%2C4%5D%2C%22current%22%3A4%7D'
            }
    def start_requests(self):
        DataSpider.cursor.execute("select * from links where Status = 'Pending'")
        links = DataSpider.cursor.fetchall()
        # url = 'https://t.restaurantguru.com/La-Empana-De-Duao-Chile-3'
        # yield scrapy.Request(url=url, callback=self.parse, headers=DataSpider.headers)    
        for link in links:
            yield scrapy.Request(url=link[1], callback=self.parse, headers=DataSpider.headers, meta={'url': link[1]})
    
    def parse(self, response):
        item = RestaurantguruData()

        url = response.meta['url']
        restaurant = response.xpath('//h1[@class="notranslate"]/a/text()').get()
        if '"' and "'" in restaurant:
            restaurant = restaurant.replace("'", '')
        
        jsonData = response.xpath('//script[@type="application/ld+json"]/text()').get()
        
        if jsonData:
            jsonData = json.loads(jsonData)
            latitude = jsonData['geo']['latitude']
            longitude = jsonData['geo']['longitude']
        
        else:
            coordinates = response.xpath('//div[@class="info_address"]/a/@href').get()
            
            if coordinates:
                coordinates = coordinates.split('n=')[1].split(',')
                latitude = coordinates[0]
                longitude = coordinates[1]

            else:
                try:
                    coordinates = response.xpath('//script[contains(text(), "map_center")]/text()').get().strip()
                    # coordinates = coordinates.find('{lat') , coordinates.find('};')
                    coordinates = coordinates[coordinates.find('{lat')+1 : coordinates.find('};')].replace('lat:', '').replace('lng:', '').strip().split(', ')
                    latitude = coordinates[0]
                    longitude = coordinates[1]
                except:
                    print(Exception)

        item['url'] = url
        item['restaurant'] = restaurant
        item['latitude'] = latitude
        item['longitude'] = longitude

        yield item