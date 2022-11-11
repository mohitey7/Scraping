import scrapy, sys, json
sys.path.append('../pedidosya')
from scrapy.cmdline import execute
from unidecode import unidecode
from pedidosya.dbconfig import ConfigSql
from pedidosya.items import PedidosyaLinks


class LinksSpider(scrapy.Spider):
    name = 'links'
    handle_httpstatus_list = [403, 429, 500, 502]
    cookies = {
    '_pxvid': '32dc024c-6025-11ed-b97e-61547a517771',
    '_pxhd': 'Gf8PF7vJA0wmPtfMhdQsRMavdehTsU689DCGBitDo-xI-ow8yaJT4ATq0JItCazUeDX3IWVIW7JAEwklgBm-/g==:7b950MFHeM9bgs4Sv/dg7vg/iFhaP0RxQ-OWbA891FTGT1XJU9wUNrkmIwH8Mv0Rr4QGMJo6mi-L0XNkg00phF/5yjhx3zgj5EiGhacjXrM=',
    'pxcts': '92bb8821-60d8-11ed-8f74-6e7670444478',
    'cf_clearance': 'aaa6f28a1c2c681d2e001ccff3043b4d311ae628-1668079988-0-250',
    'dhhPerseusGuestId': '1668079989422.148923433062768220.dxe2us2t28',
    'dhhPerseusSessionId': '1668079989422.167458705507920420.ln0fo043t99',
    '__Secure-peya.sid': 's%3A5854f014-292a-4378-8855-4bdcc20a915b.0bhGoDDCcDqNkZj%2BuY5H9HeWdY4rFCQ%2BbPfgb3DNXpI',
    '__Secure-peyas.sid': 's%3A1011c69c-605b-4793-922d-98f11153f747.tj5oKrEO9xGKyMOG7d4hRbRao1pVamNuSUfuVvtlA5w',
    '__cf_bm': 'CEiwuDzu58DqFQ2YF1nAp585y0QGeP2HMEKBWMAaAxM-1668079990-0-AUzlEkrrB1Ci6FYuH6PjMXlvwpU07xFaVr3xpG9bNHLTIwqyxQfPZ2RYJdn0bUye0sjqBsSppk2jiGo6ZHFrSYI=',
    'cf_chl_prog': 'F14',
    'cf_chl_rc_i': '1',
    '_px3': '1af919c654bdd8db28c86cbc76464228e2422d5bf0e131fe77f91e798ef661dd:Dn6+IhKs+3H3DYadVjF7pEOsPHB4ZRXkdq04fmvR2woQOd5uvhuiUFk717uh0+nASoVvdFXxKIGOMFNyFg6dzA==:1000:d9EM8mtLEGnaWxXeChFCcJrqr+/wz7O5GNrEi4Wxs5zN/mDjSbjR8OFcfvL3yTTT5OFPp3nK6Z+jTaSBnSJcEEl6cJMV4YPuJyYpcnHnEJKdyJx1BYCnHFn0bwhK8VXBRA1jq4P2wsILhzMa6YhUJ4z5Mt9JZn+ELfbrYr98nQq1pHZAZO+A6efCN2/q4pG5JDbXDw75EvLNdc+uV+wjiA==',
    'dhhPerseusHitId': '1668080505288.140481986476762580.k9idllahhqs',
}

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'referer': ' https://www.pedidosya.cl/restaurantes?address=Chile%20Espa%C3%B1a&city=Santiago&lat=-33.4470043&lng=-70.59684539999999',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
        }
    
    def start_requests(self):
        self.geocodes = ConfigSql(database='pedidosya', table='geocodes')
        self.geocodes.dictcursor.execute(f"select * from {self.geocodes.table} where Status = 'Pending'")
        
        categories = ['Restaurant', 'Groceries', 'Drinks', 'Coffee', 'Pharmacy', 'Pets', 'Shop']
        
        for i in self.geocodes.dictcursor.fetchall():
            for category in categories:
                url = f"https://www.pedidosya.cl/mobile/v5/shopList?businessType={category}&country=2&max=2000&offset=0&point={i['Latitude']}%2C{i['Longitude']}"
                yield scrapy.Request(url=url, headers=LinksSpider.headers, cookies=LinksSpider.cookies, callback=self.parse, dont_filter=True, meta={'category': category, 'Latitude': i['Latitude'], 'Longitude': i['Longitude']})

    def parse(self, response):
        if response.status == 200:
            item = PedidosyaLinks()

            jsonData = json.loads(response.text)
            totalcount = jsonData['swimlanes']['info']['totalRestaurants']

            if 0 < totalcount < 2000:
                for i in jsonData['list']['data']:
                    city = unidecode(i['cityName'].casefold())
                    if ' ' in city:
                        city = city.replace(' ', '-')
                    item['URL'] = f"https://www.pedidosya.cl/restaurantes/{city}/{i['link']}-menu"
                    item['Name'] = i['name']
                    item['City'] = i['cityName']
                    item['Latitude'] = i['latitude']
                    item['Longitude'] = i['longitude']
                    item['Category'] = response.meta['category']
                    
                    yield item

                self.geocodes.dictcursor.execute(f"update {self.geocodes.table} set Status = 'Done' where Latitude = '{response.meta['Latitude']}' and Longitude = '{response.meta['Longitude']}'")
                self.geocodes.connection.commit()

            elif totalcount > 2000:
                print("Totalcount higher!")
            
            else:
                self.geocodes.dictcursor.execute(f"update {self.geocodes.table} set Status = 'None' where Status = 'Pending' and Latitude = '{response.meta['Latitude']}' and Longitude = '{response.meta['Longitude']}'")
                self.geocodes.connection.commit()

        else:
            yield scrapy.Request(url=response.url, headers=LinksSpider.headers, cookies=LinksSpider.cookies, callback=self.parse, dont_filter=True, meta={'category': response.meta['category'], 'Latitude': response.meta['Latitude'], 'Longitude': response.meta['Longitude']})

if __name__ == '__main__':
    execute("scrapy crawl links".split())