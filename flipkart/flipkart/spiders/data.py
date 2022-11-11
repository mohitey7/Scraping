import scrapy, pymysql, json
from ..items import FlipkartData
from datetime import datetime

class DataSpider(scrapy.Spider):
    name = 'data'
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='flipkart')
    cursor = connection.cursor()
    
    def start_requests(self):
        DataSpider.cursor.execute("select * from links where Status = 'Pending'")
        links = DataSpider.cursor.fetchall()

        for link in links:
            yield scrapy.Request(url=link[1], callback=self.parse, meta={'url': link[1]})
    
    def parse(self, response, **kwargs):
        item = FlipkartData()

        rawJson = response.xpath('//script[@id="is_script"]/text()').get().replace("window.__INITIAL_STATE__ = ", '')[:-1]
        jsonData = json.loads(rawJson)

        # url
        url = response.meta['url']
        
        # producttitle
        try:
            productTitle = jsonData['pageDataV4']['page']['pageData']['pageContext']['titles']['title']  + " " + f"({jsonData['pageDataV4']['page']['pageData']['pageContext']['titles']['subtitle']})"
        except:
            try:
                productTitle = jsonData['pageDataV4']['page']['pageData']['pageContext']['titles']['title']
            except:
                try:
                    for i in jsonData['pageDataV4']['page']['data']['10002']:
                        try:
                            if "PAGE_SUMMARY" in i['elementId']:
                                try:
                                    productTitle = i['widget']['data']['titleComponent']['value']['title'] + " " + f"({i['widget']['data']['titleComponent']['value']['subtitle']})"
                                except:
                                    productTitle = i['widget']['data']['titleComponent']['value']['title']
                        except:
                            pass
                except:
                    productTitle = "None"

        scrappedTime = datetime.now().strftime('%I:%M:%S %p, %d/%m/%Y')

        # category
        try:
            for i in jsonData['pageDataV4']['page']['pageData']['seoData']['schema']:
                if i['@type'] == "BreadcrumbList":
                    category = []
                    for j in i['itemListElement']:
                        category.append(j['item']['name'])
            category.pop(0)
            category.pop(-1)
            category = ' | '.join(category)
        except:
            category = "None"
        
        # product price
        try:
            price = jsonData['pageDataV4']['page']['pageData']['pageContext']['pricing']['finalPrice']['value']
        except:
            price = "None"

        # images
        try:
            for i in jsonData['pageDataV4']['page']['data']['10001']:
                try:
                    if i['widget']['type'] == "MULTIMEDIA":
                        images = []
                        for j in i['widget']['data']['multimediaComponents']:
                            images.append(j['value']['url'].replace('/{@width}/{@height}', '').replace('?q={@quality}', ''))
                except:
                    pass
            images = ' | '.join(images)
        except:
            images = "None"

        # averagerating --- pageDataV4.page.data[10002][1].widget.data.ratingsAndReviews.value.rating.average
        try:
            averageRating = jsonData['pageDataV4']['page']['pageData']['pageContext']['rating']['average']
        except:
            averageRating = "None"
        # for i in jsonData['pageDataV4']['page']['data']['10002']:
        #     try:
        #         if "PRODUCT_PAGE_SUMMARY" in i['elementId']:
        #             averageRating = i['widget']['data']['ratingsAndReviews']['value']['rating']['average']
        #     except:
        #         pass
        # else:
        #     averageRating = "None"
        
        # ratings = pageDataV4.page.pageData.pageContext.trackingDataV2.ratingCount
        try:
            ratings = jsonData['pageDataV4']['page']['pageData']['pageContext']['trackingDataV2']['ratingCount']
        except:
            ratings = "None"
        
        # ratings = pageDataV4.page.pageData.pageContext.trackingDataV2.reviewCount
        try:
            reviews = jsonData['pageDataV4']['page']['pageData']['pageContext']['trackingDataV2']['reviewCount']
        except:
            reviews = "None"
        
        # discount = pageDataV4.page.pageData.pageContext.pricing.totalDiscount
        try:
            discount = jsonData['pageDataV4']['page']['pageData']['pageContext']['pricing']['totalDiscount']
            discount = str(discount) + "%"
        except:
            discount = "None"
        
        # availablity = pageDataV4.page.data[10002][2].widget.data.announcementComponent.value.title
        try:
            for i in jsonData['pageDataV4']['page']['data']['10002']:
                try:
                    if "AVAILABILITY" in i['elementId'] and i['widget']['data']['announcementComponent']['value']['title'] == 'Sold Out':
                        availablity = "No"
                        break
                except:
                    pass
            else:
                availablity = "Yes"
        except:
            availablity = "Yes"

        item['productTitle'] = productTitle
        item['scrappedTime'] = scrappedTime
        item['category'] = category
        item['price'] = price
        item['images'] = images
        item['averageRating'] = averageRating
        item['ratings'] = ratings
        item['reviews'] = reviews
        item['discount'] = discount
        item['availablity'] = availablity
        item['url'] = url

        print(item)
        # yield item