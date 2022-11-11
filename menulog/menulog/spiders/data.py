import scrapy, pymysql, json, re, os, hashlib
from ..items import MenulogData
from scrapy.cmdline import execute

class DataSpider(scrapy.Spider):
    name = 'data'
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='menulog')
    cursor = connection.cursor()

    def start_requests(self):
        DataSpider.cursor.execute("select * from `links3` where Status = 'Pending'")
        links = DataSpider.cursor.fetchall()

        headers = {
                'authority': 'www.menulog.co.nz',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.6',
                'cache-control': 'max-age=0',
                'cookie': 'je-auser=49cbbc64-92ac-44e6-897c-d3248ddae3e9; je-banner_cookie=130315; je-user_percentage=48; je-srv-cw=production; cf_clearance=LOPfBoc6tyBe1Ai_DA32Y4I7XnS5vGvmKoyN1pr81Kg-1663139899-0-250; je-location=0793; je-location-nz=0793; je-x-checkout=reserve; je-x-bdfe=reserve; je-x-scs=reserve; je-x-ra=reserve; je-x-csd=reserve; je-x-rra=enabled; __cf_bm=vvUSlEc6g1wCDYx.og_AvfO0xqvL87OzSA0MUajsq6U-1663154308-0-AZqc3nYBudMk4TBLi+jbc0pB5zRi3xW42CvMW32v52hs2Vhe8DM5siFAJXz7ouUXlJFaLNJH55CI05SgASCRcWE=; x-je-conversation=f630810e-b3de-421e-bb61-4e734c43bd72',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'sec-gpc': '1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
            }
        for link in links:
            yield scrapy.Request(url=link[1], callback=self.parse, meta={'url': link[1]}, headers=headers)
        # url = 'https://www.menulog.co.nz/restaurants-taj-spice/menu'
        # yield scrapy.Request(url=url, callback=self.parse, meta={'url': url}, headers=headers)

    def parse(self, response):
        
        item = MenulogData()

        url  = response.url
        hashid = int(hashlib.md5(bytes(str(url), "utf8")).hexdigest(), 16) % (10 ** 10)
        item['hashid'] = hashid

        try:
            jsonData = response.xpath('//script[contains(text(),"__INITIAL_STATE__")]//text()').get()
            jsonData = re.findall('__INITIAL_STATE__=(.*)', jsonData)[0]
            jsonData = json.loads(jsonData)

            item['url'] = response.meta['url']

            #city = state.restaurantInfo.location.city
            try:
                item['city'] = jsonData['state']['restaurantInfo']['location']['city']
            except:
                item['city'] = ""

            # name = state.restaurantInfo.name
            try:
                item['name'] = jsonData['state']['restaurantInfo']['name']
            except:
                item['name'] = ""

            # cuisines = state.restaurantInfo.cuisineTypes[0].name
            try:
                cuisines = []
                for i in jsonData['state']['restaurantInfo']['cuisineTypes']:
                    cuisines.append(i['name'])
                try:
                    if jsonData['state']['restaurantInfo']['isHalal'] is True:
                        cuisines.append("Halal")
                except:
                    pass
                if len(cuisines)>1:
                    item['cuisines'] = ' | '.join(cuisines)
                else:
                    item['cuisines'] = ''.join(cuisines)
            except:
                item['cuisines'] = ""

            # rating = state.restaurantInfo.ratingAverageStars
            try:
                item['rating'] = jsonData['state']['restaurantInfo']['ratingAverageStars']
            except:
                item['rating'] = ""

            # review = state.restaurantInfo.ratingCount
            try:
                item['review'] = jsonData['state']['restaurantInfo']['ratingCount']
            except:
                item['review'] = ""

            # streetAddress = state.restaurantInfo.location.address
            try:
                item['streetAddress'] = jsonData['state']['restaurantInfo']['location']['address']
            except:
                item['streetAddress'] = ""

            # postcode = state.restaurantInfo.location.postCode
            try:
                item['postcode'] = jsonData['state']['restaurantInfo']['location']['postCode']
            except:
                item['postcode'] = ""
            
            # FullAddress
            item['FullAddress'] = item['streetAddress'] + ', ' + item['postcode']

            try:
                jsonData2 = response.xpath('//script[@type="application/ld+json"]/text()').get()
                jsonData2 = json.loads(jsonData2)
                item['addressLocality'] = jsonData2['address']['addressLocality']
            except:
                item['addressLocality'] = ''

            deliveryTime = response.xpath('//span[@class="c-basketSwitcher-eta"]/text()').get()
            if deliveryTime:
                item['deliveryTime'] = deliveryTime.strip()
            else:
                item['deliveryTime'] = ""

            # restaurantID = state.restaurantId
            try:
                item['restaurantID'] = jsonData['state']['restaurantId']
            except:
                item['restaurantID'] = ""

            # phone = state.restaurantInfo.allergenPhoneNumber
            try:
                item['phone'] = jsonData['state']['restaurantInfo']['allergenPhoneNumber']
            except:
                item['phone'] = ""

            # note = state.menu.menus[0].description
            try:
                note = jsonData['state']['menu']['menus'][0]['description']
                if note and len(note)>0:
                    item['note'] = note.strip()
                else:
                    item['note'] = ""
            except:
                item['note'] = ""

            # about = state.restaurantInfo.description
            try:
                item['about'] = jsonData['state']['restaurantInfo']['description']
                item['about'] = item['about'].strip()
            except:
                item['about'] = ""

            # state.restaurantInfo.restaurantOpeningTimes[0].timesPerDay[0].times
            try:
                for i in jsonData['state']['restaurantInfo']['restaurantOpeningTimes']:
                    if i['serviceType'] == 'delivery':
                        final_string = ""
                        for j in i['timesPerDay']:
                            final_string += f"{j['dayOfWeek']} : "
                            if len(j['times']) > 0:
                                for k in j['times']:
                                    if len(j['times']) > 1:
                                        string_time = f"{k['fromLocalTime']} - {k['toLocalTime']}"
                                        final_string += f"{string_time}, "
                                    elif len(j['times']) == 1:
                                        string_time = f"{k['fromLocalTime']} - {k['toLocalTime']}"
                                        final_string += f"{string_time}"
                                    else:
                                        final_string += "Closed,"
                            else:
                                final_string += "Closed "
                                    
                            final_string += " | "

                final_string = final_string.replace(',  |', ' |')
                final_string = final_string[:-3]
                if final_string[-2] == ',':
                    final_string = final_string[:-2]
                item['deliveryhours'] = final_string
            except:
                item['deliveryhours'] = ""

            #latitude = state.restaurantInfo.location.latitude
            try:
                item['latitude'] = jsonData['state']['restaurantInfo']['location']['latitude']
            except:
                item['latitude'] = ""

            # longitude = state.restaurantInfo.location.longitude
            try:
                item['longitude'] = jsonData['state']['restaurantInfo']['location']['longitude']
            except:
                item['longitude'] = ""
            
            path = "D:/Mohit Aswani/HTML/"
            if not os.path.exists(path):
                os.makedirs(path)
            with open(f"{path}{item['restaurantID']}.html", 'wb') as f:
                f.write(response.body)

            yield scrapy.Request(url=item['url'], callback=self.delivery, meta={'item':item}, dont_filter=True)
        
        except Exception as e:
            print('first_function error',e)

    def delivery(self, response):
        
        try:
            item = response.meta['item']
            headers = {
                    'authority': 'aus.api.just-eat.io',
                    'accept': 'application/json, text/plain, */*',
                    'accept-language': 'en-US,en;q=0.6',
                    'origin': 'https://www.menulog.co.nz',
                    'referer': 'https://www.menulog.co.nz/',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'cross-site',
                    'sec-gpc': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
                }

            url = f"https://aus.api.just-eat.io/restaurant/nz/{item['restaurantID']}/menu/dynamic"
            
            yield scrapy.Request(url=url, headers=headers, callback=self.parse2, meta={'item':item}, dont_filter=True)
        
        except:
            print('second function error')
    
    def parse2(self, response):
        item = response.meta['item']
        jsonData3 = json.loads(response.text)

        path = "D:/Mohit Aswani/MinOrder & Delivery fee/"
        if not os.path.exists(path):
            os.makedirs(path)
        with open(f"{path}{item['restaurantID']}.html", 'wb') as f:
            f.write(response.body)
        
        # minimumamount = DeliveryFees.MinimumOrderValue
        try:
            item['minimumAmount'] = jsonData3['DeliveryFees']['MinimumOrderValue']
            if item['minimumAmount']:
                item['minimumAmount'] = item['minimumAmount'] / 100
        except:
            item['minimumAmount'] = ""

        # DeliveryFee = DeliveryFees.Bands[0].Fee
        try:
            deliveryFee = ""
            for i in reversed(jsonData3['DeliveryFees']['Bands']):
                if len(jsonData3['DeliveryFees']['Bands']) > 1:
                    deliveryFee += f"{i['Fee']/100} - "
                else:
                    deliveryFee += f"{i['Fee']/100}"
            
            if deliveryFee[-3:] == " - ":
                deliveryFee = deliveryFee[:-3]
            item['DeliveryFee'] = deliveryFee
        except:
            item['DeliveryFee'] = ""

        stamp_url = f"https://aus.api.just-eat.io/consumeroffers/notifications/au?restaurantIds={item['restaurantID']}&optionalProperties=offerMenuItems"
        
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-AU',
            'origin': 'https://www.menulog.com.au',
            'referer': 'https://www.menulog.com.au/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
            'x-je-auser': '3a4b546a-8410-4fb9-880e-24ef8b877d61',
            'x-je-conversation': '7d09a58e-e130-4f10-99ad-6a77d53ff912',
            'x-je-feature': 'menuweb',
            'Cookie': '__cf_bm=YaAu6hu2zfkuY6BFK2oPzWcPv8EPVrrEjdyX6rP9QZ8-1663158341-0-AS9LV23XdcY3IiXaFa7CLfUeYuPJnr0hDqX6EzVZDvJqEJBpeGUF6tVguEHJxrUkJuFYKKzFRQe9nVAzZJvU7k4='
        }
        yield scrapy.Request(url=stamp_url, headers=headers, callback=self.stampcard, meta={'item': item}, dont_filter=True)
   
    def stampcard(self, response):
        item = response.meta['item']

        path = "D:/Mohit Aswani/Stampcard & Offers/"
        if not os.path.exists(path):
            os.makedirs(path)
        with open(f"{path}{item['restaurantID']}.html", 'wb') as f:
            f.write(response.body)
        
        try:
            jsonData = json.loads(response.text)
            StampCard = jsonData['offerNotifications'][0]['offerType']
            item['StampCard'] = StampCard
        except Exception as e:
            item['StampCard'] = ''
        
        try:
            item['offer'] = jsonData['offerNotifications'][0]['description']
        except:
            item['offer'] = ''
        
        yield item

if __name__ == '__main__':
    execute('scrapy crawl data'.split())