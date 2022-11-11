import scrapy, math, hashlib
from scrapy.utils.response import open_in_browser
from ..items import RestaurantguruLinks

class LinksSpider(scrapy.Spider):
    name = 'links'
    
    headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'sec-gpc': '1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
            }
    
    def start_requests(self):

        price = [1, 2, 3, 4]

        # main page categories

        # restaurants = 'best-restaurants-Chile-b'
        # fastFood = 'fast-food-Chile-t11'
        # pubsBars = 'pub-and-bar-Chile-t2'
        # chilean = 'cuisines=19'
        # chinese = 'chinese-Chile-c20'
        # vegetarian = 'vegetarian-Chile-c93'
        # steaks = 'steakhouse-Chile-t4'
        # pizza = 'pizza-Chile-c97'
        # seafood = 'seafood-restaurants-Chile-b'
        # bbq = 'barbecue-Chile-c164'
        # desserts = 'desserts-Chile-t12'
        # sushi = 'sushi-Chile-c98'
        # breakfast = 'breakfast-Chile-m9619'
        # dinner = 'dinner-Chile-m9623'
        # businessLunch = 'business-lunch-Chile-m9620'

        # categories = [restaurants, fastFood, pubsBars, chilean, chinese, vegetarian, steaks, pizza, seafood, bbq, desserts, sushi, breakfast, breakfast, dinner, businessLunch]

        # for i in price:
        #     for j in categories:
        #         if j != 'cuisines=19':
        #             categoryUrl = f'https://t.restaurantguru.com/{j}?&price_score={i}'
        #         else:
        #             categoryUrl = f'https://t.restaurantguru.com/restaurant-Chile-t1?{j}&price_score={i}'
        #         yield scrapy.Request(url=categoryUrl, callback=self.parse, meta={'categoryUrl': categoryUrl}, headers=LinksSpider.headers)

        # --------------------------------------------------------------------------------------------------------------------------------------------------------
        
    # For Cuisines
        cuisines = [169, 98, 19, 97, 80, 177, 67, 45, 20, 43, 171, 83, 49, 179, 93, 40, 7, 173, 25, 3, 191, 187, 53, 33, 52, 186, 182, 32, 94, 165, 164, 81, 34, 190, 54, 5, 87, 21, 30, 17, 180, 59, 178, 38, 184, 22, 10, 167, 47, 13, 18, 90, 170, 192, 35, 46, 50, 23, 162, 95, 9, 196, 71, 11, 36, 1, 2, 188, 176, 27, 86, 28, 41, 8, 181, 64, 66, 161, 39, 56, 65, 194, 72, 163, 172, 82, 6]
        
        for i in price:
            for j in cuisines:
                categoryUrl = f'https://t.restaurantguru.com/restaurant-Chile-t1?cuisines={i}&price_score={i}'
                yield scrapy.Request(url=categoryUrl, callback=self.parse, meta={'categoryUrl': categoryUrl}, headers=LinksSpider.headers)

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------

    # For Dishes
        dishCodes = [10984, 6, 5713, 6520, 11, 8094, 4, 9289, 102, 5937, 94, 5767, 326, 8150, 14, 5714, 32, 5734, 13, 11313, 10443, 28, 93, 8295, 10164, 5715, 12, 6525, 27, 29, 6528, 265, 31, 5731, 6924, 6428, 2, 7830, 10485, 10521, 10, 85, 3, 6552, 10976, 1, 8044, 138, 18, 11027, 8485, 8280, 6938, 33, 7396, 8725, 8763, 90, 76, 8432, 6888, 6755, 11315, 5947, 6978, 8090, 237, 11312, 5733, 97, 6527, 5778, 5719, 10876, 11321, 6554, 8073, 105, 7888, 10965, 40, 17, 5718, 7959, 51, 42, 9726, 817, 6427, 5750, 7092, 69, 8385, 6541, 5954, 7157, 10956, 7466, 9963, 9074, 6001, 5730, 5728, 5976, 8095, 7765, 11325, 6809, 8452, 387, 7012, 6548, 11326, 7309, 5752, 869, 10126, 73, 7948, 6526, 7548, 7696, 8076, 5751, 9527, 6558, 10493, 823, 6876, 6860, 235, 6757, 131, 109, 11316, 10053, 8109, 6561, 9697, 6530, 5744, 6804, 6438, 7280, 150, 121, 10089, 7011, 6797, 24]

        for i in price:
            for j in dishCodes:
                categoryUrl = f'https://t.restaurantguru.com/restaurant-Chile-t1?meals={j}&price_score={i}'
                yield scrapy.Request(url=categoryUrl, callback=self.parse, meta={'categoryUrl': categoryUrl}, headers=LinksSpider.headers)

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------

    # For Features
        features = [9635, 9631, 9622, 9642, 9619, 9643, 9640, 9625, 9644, 9630, 9623, 9633, 9638, 9621, 9632, 9626, 9628, 9634, 9637, 9620, 9645]

        for i in price:
            for j in features:
                categoryUrl = f'https://t.restaurantguru.com/restaurant-Chile-t1?meals={j}&price_score={i}'
                yield scrapy.Request(url=categoryUrl, callback=self.parse, meta={'categoryUrl': categoryUrl}, headers=LinksSpider.headers)

    # -------------------------------------------------------------------------------------------------------------------------------------------------------------
    
    # For PageUrl
    def parse(self, response):
        totalPages = response.xpath('//span[@class="filter_count"]/text()').get()
        totalPages = int(totalPages.split(' ')[0])
        categoryUrl = response.meta['categoryUrl']

        if totalPages:
            totalPages = math.ceil(int(totalPages)/20)

            for i in range(1, totalPages+1):
                pageUrl = categoryUrl.split('?')[0] + f'/{i}?' + categoryUrl.split('?')[1]
                yield scrapy.Request(url=pageUrl, callback=self.parse2, meta={'categoryUrl': categoryUrl, 'pageUrl': pageUrl}, headers=LinksSpider.headers)

    # -------------------------------------------------------------------------------------------------------------------------------------------------------------
    
    # For RestroUrl
    def parse2(self, response):
        item = RestaurantguruLinks()
        
        categoryUrl = response.meta['categoryUrl']
        pageUrl = response.meta['pageUrl']
        
        links = response.xpath('//a[@class="notranslate title_url"]/@href').getall()
        
        for link in links:
            item['categoryUrl'] = categoryUrl
            item['pageUrl'] = pageUrl
            item['restroUrl'] = link

            hashStr = item['categoryUrl'] + item['pageUrl'] + item['restroUrl']
            hashID = int(hashlib.md5(bytes(str(hashStr), "utf8")).hexdigest(), 16) % (10 ** 10)
            
            item['hashID'] = hashID
            
            yield item

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------

    
    # def parse2(self, response):
    #     item = RestaurantguruLinks()
    #     cuisineUrl = response.meta['cuisineUrl']
    #     pageUrl = response.meta['pageUrl']
    #     links = response.xpath('//a[@class="notranslate title_url"]/@href').getall()
        
    #     for link in links:
    #         item['restroUrl'] = link
    #         yield item