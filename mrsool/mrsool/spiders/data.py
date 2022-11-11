import scrapy, pymysql, json, hashlib
from ..items import MrsoolData

class DataSpider(scrapy.Spider):
    name = 'data'
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='mrsool')
    cursor = connection.cursor()

    def start_requests(self):
        DataSpider.cursor.execute("select * from links where Status = 'Pending'")
        links = DataSpider.cursor.fetchall()
        # url = 'https://business-api.mrsool.co/v1/business_orders/refactored_new?latitude=24.713268&longitude=46.673428&shop_id=c505c6cd45fc0c4dcb98f0ffda57dcde' + '&language=EN'
        # yield scrapy.Request(url=url, method="POST", callback=self.parse)
        for link in links:
            url = link[1] + '&language=EN'
            yield scrapy.Request(url=url, meta={'url': url, 'restaurant': link[2], 'ourl':link[1]}, method="POST", callback=self.parse)

    def parse(self, response):
        item = MrsoolData()
        url2 = response.meta['ourl']

        url = response.meta['url']
        restaurant = response.meta['restaurant']
        
        jsonData = json.loads(response.text)
        
        if jsonData['data']:
            categories = jsonData['data']['menu_item_category']
            try:
                for i in categories:
                    subcategories = i['menu_items']
                    if len(subcategories) > 0:
                        category = i['category_name']
                        if category is None:
                            category = ""
                        for j in subcategories:
                            menuitem = j['name']
                            if menuitem is None:
                                menuitem = ""
                            longdesc = j['long_desc']
                            if '"' and "'" in longdesc:
                                longdesc = longdesc.replace("'", "’")
                            if longdesc is None:
                                longdesc = ""
                            
                            price = j['price']
                            if price is None:
                                price = ""
                            
                            hashID = restaurant + category + menuitem + url
                            hashID = hashlib.md5(hashID.encode()).hexdigest()

                            item['url'] = url
                            item['restaurant'] = restaurant.strip()
                            item['category'] = category.strip()
                            item['menuitem'] = menuitem.strip()
                            item['longdesc'] = longdesc.strip()
                            item['price'] = price
                            item['hashID'] = hashID
                
                            yield item
                
                DataSpider.cursor.execute(f"UPDATE `links` SET `Status` = 'Completed' WHERE `URL` = '{url2}'")
                DataSpider.connection.commit()
            
            except Exception as e:
                print(e)
        
        else:
            print("No data in URL!")