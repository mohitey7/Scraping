import scrapy, json
from scrapy.cmdline import execute
from ..items import MenulogLinks


class LinksSpider(scrapy.Spider):
    name = 'links'

    def start_requests(self):
        url = 'https://www.menulog.co.nz/takeaway'
        headers = {
                    'authority': 'www.menulog.co.nz',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'accept-language': 'en-US,en;q=0.6',
                    'cache-control': 'max-age=0',
                    'cookie': 'je-auser=49cbbc64-92ac-44e6-897c-d3248ddae3e9; je-banner_cookie=130315; je-user_percentage=48; je-srv-cw=production; je-location=7700; je-location-nz=7700; cf_chl_rc_i=2; cf_chl_2=7adda620939e2d4; cf_chl_prog=x14; cf_clearance=LOPfBoc6tyBe1Ai_DA32Y4I7XnS5vGvmKoyN1pr81Kg-1663139899-0-250; __cf_bm=bb4PTynfoeECjgldlMMtvppN38XU1fd2HaY2g.broWo-1663139995-0-ARBCHFKJirp2AkCdHAZoyzFxE5n/L7Y8g0zkxbZJCFnTJCnTOpcRZIdNOP6Afxh5t+eQsF30C2wvcyavyo6Hef0=; _dd_s=logs=1&id=1396e3c3-8c3f-4623-9c62-ae00165ccc1a&created=1663138388240&expire=1663141079480',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-user': '?1',
                    'sec-gpc': '1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
                }
        yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response):
        cities = response.xpath('//li[@class="link-item"]/a[contains(@href, "/takeaway")]/@href').getall()
        for i in range(len(cities)):
            cities[i] = 'https://www.menulog.co.nz' + cities[i]
            yield scrapy.Request(url=cities[i], callback=self.parse2)

    def parse2(self, response):
        try:
            jsonData = json.loads(response.xpath('//script[@id="__JSS_STATE__"]/text()').get())
        except:
            if "We're coming up empty" in response.text:
                print("Restaurant not found in current city!")

        # sitecore.route.placeholders.main[6].fields.groups[0].Links[0].Url
        try:
            for i in jsonData['sitecore']['route']['placeholders']['main']:
                if i['componentName'] == "District List":
                    for j in i['fields']['groups']:
                        for k in j['Links']:
                            locality = k['Url']
                            yield scrapy.Request(url=locality, callback=self.parse3)
        except:
            pass

    def parse3(self, response):
        item = MenulogLinks()
        jsonData = response.xpath('//script[contains(text(), "__INITIAL_STATE__")]/text()').get().replace('window["__INITIAL_STATE__"] = JSON.parse("', '').replace('");', '')
        jsonData = jsonData.strip().encode('utf-8').decode('unicode-escape')
        jsonData = json.loads(jsonData)

        try:
            for i in jsonData['restaurants']:
                url = jsonData['restaurants'][i]['uniqueName']
                url = 'https://www.menulog.co.nz/restaurants-' + url + '/menu'
                item['url'] = url
                yield item
        except:
            pass

# if __name__ == '__main__':
#     execute('scrapy crawl links'.split())