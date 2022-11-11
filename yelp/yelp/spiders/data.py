import scrapy, pymysql, re, time
from ..items import YelpData

class DataSpider(scrapy.Spider):
    name = 'data'
    connection = pymysql.connect(host='localhost', user='root', password='workbench', database='yelp')
    cursor = connection.cursor()

    headers = {
    'authority': 'www.yelp.co.uk',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'accept-language': 'en-IN,en;q=0.6',
    'cache-control': 'max-age=0',
    # Requests sorts cookies= alphabetically
    # 'cookie': 'wdi=1|A7CA69CFC95E5BCF|0x1.8c4bf9faae37ep+30|9633edef56b4e533; hl=en_GB; bse=cde5caf065b24373bc35b10ad331af34; recentlocations=; location=%7B%22latitude%22%3A+51.51272415%2C+%22country%22%3A+%22GB%22%2C+%22place_id%22%3A+%221209%22%2C+%22longitude%22%3A+-0.13540315%2C+%22state%22%3A+%22XGL%22%2C+%22provenance%22%3A+%22YELP_GEOCODING_ENGINE%22%2C+%22unformatted%22%3A+%22united+kingdom%22%2C+%22accuracy%22%3A+4%2C+%22max_longitude%22%3A+-0.02239919999999529%2C+%22min_latitude%22%3A+51.4406367%2C+%22address1%22%3A+%22%22%2C+%22parent_id%22%3A+1181%2C+%22address2%22%3A+%22%22%2C+%22zip%22%3A+%22%22%2C+%22location_type%22%3A+%22locality%22%2C+%22max_latitude%22%3A+51.5848116%2C+%22city%22%3A+%22London%22%2C+%22display%22%3A+%22London%22%2C+%22min_longitude%22%3A+-0.24840710000000854%2C+%22address3%22%3A+%22%22%2C+%22borough%22%3A+%22%22%2C+%22county%22%3A+null%2C+%22isGoogleHood%22%3A+false%2C+%22language%22%3A+null%2C+%22neighborhood%22%3A+%22%22%2C+%22polygons%22%3A+null%2C+%22usingDefaultZip%22%3A+false%2C+%22confident%22%3A+null%7D; pid=1fcb926af00e810c; xcj=1|xa_kwwN555j6FXlVFslcQWe0P0Ty59zpY9R442a1fMA; OptanonConsent=isGpcEnabled=1&datestamp=Sun+Sep+04+2022+16%3A22%3A33+GMT%2B0530+(India+Standard+Time)&version=6.28.0&isIABGlobal=false&hosts=&consentId=41751c77-feb7-443f-a669-daa0b78e6d52&interactionCount=1&landingPath=NotLandingPage&groups=BG51%3A1%2CC0003%3A1%2CC0002%3A1%2CC0001%3A1%2CC0004%3A0&AwaitingReconsent=false',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'sec-gpc': '1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        DataSpider.cursor.execute("select * from `company urls` where Status = 'Pending'")
        links = DataSpider.cursor.fetchall()

        for link in links:
            yield scrapy.Request(url=link[1], headers=DataSpider.headers, callback=self.parse)

    def parse(self, response):
        if response.status != 200:
            time.sleep(2)
            yield scrapy.Request(url=response.url, headers=DataSpider.headers, callback=self.parse)
        
        item = YelpData()

        all_urls = response.xpath('//div[@class=" arrange-unit__09f24__rqHTg arrange-unit-fill__09f24__CUubG border-color--default__09f24__NPAKY"]//a[@class="css-1um3nx"]/@href').getall()

        if len(all_urls)>0:
            for i in all_urls:
                if i.startswith('/biz'):
                    website = i
                    break
            website = re.search('F%2F(.*)&c', website).group(1).replace('%2F', '')
        else:
            website = "None"

        company = response.xpath('//h1[@class="css-dyjx0f"]/text()').get()
        if company:
            company = company.strip()
        else:
            company = "None"
        
        contact_number = response.xpath('//div[@class=" arrange-unit__09f24__rqHTg arrange-unit-fill__09f24__CUubG border-color--default__09f24__NPAKY"]/p[@class=" css-1p9ibgf"]/text()').get()
        if contact_number:
            contact_number = contact_number.replace(' ', '').replace('-', '')
        else:
            contact_number = "None"

        address = response.xpath('//div[@class=" arrange-unit__09f24__rqHTg arrange-unit-fill__09f24__CUubG border-color--default__09f24__NPAKY"]/p[@class=" css-qyp8bo"]/text()').get()
        if address:
            address = address.strip()
        else:
            address = "None"

        business_owner = response.xpath('//div[@class=" arrange-unit__09f24__rqHTg arrange-unit-fill__09f24__CUubG border-color--default__09f24__NPAKY"]/p[@class=" css-ux5mu6"]/text()').get()
        if business_owner:
            business_owner = business_owner.strip(".")
        else:
            business_owner = "None"

        item['website'] = website
        item['company'] = company
        item['contact_number'] = contact_number
        item['address'] = address
        item['business_owner'] = business_owner
        item['page_url'] = response.url

        yield item