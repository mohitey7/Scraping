import scrapy, json
from ..items import MrsoolLinks

class LinksSpider(scrapy.Spider):
    name = 'links'
    url = 'https://0ljgy5fow6-dsn.algolia.net/1/indexes/*/queries'
    headers = {
            'X-Algolia-Application-Id': '0LJGY5FOW6',
            'X-Algolia-API-Key': 'c82651591a704ee3bda934594d6ba0a1',
            'User-Agent': 'Android (22); InstantSearchAndroid (2.9.0),Algolia for Kotlin (1.7.0)',
            'Accept': 'application/json',
            'Accept-Charset': 'UTF-8',
            'Content-Type': 'text/plain; charset=UTF-8',
            'Host': '0ljgy5fow6-dsn.algolia.net',
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip'
            }

    def start_requests(self):

        payload = "{\"requests\":[{\"indexName\":\"services_production\",\"params\":\"query=&filters=%28+%22country_code%22%3A%22SA%22+AND+%22skip_distance_check%22%3A%22false%22+AND+%22mrsool_service%22%3A%22true%22+%29+AND+%28+promotion+%3E=+0+%29+AND+%28category_ids%3A166%29&optionalFilters=%5B%5B%22mapped_opening_hours%3A50300%3Cscore=2%3E%22%5D%2C%5B%22+is_online%3Atrue%3Cscore=1%3E%22%5D%5D&page=0&hitsPerPage=80&aroundLatLng=27.523647%2C41.696632&aroundRadius=all&aroundPrecision=1000&ruleContexts=%5B%22android%22%5D&distinct=1&getRankingInfo=true&clickAnalytics=true&analytics=true\"},{\"indexName\":\"services_production\",\"params\":\"query=&filters=%28+%22country_code%22%3A%22SA%22+AND+%22skip_distance_check%22%3A%22true%22+%29+AND+%28+promotion+%3E=+0+%29+AND+%28category_ids%3A166%29&optionalFilters=%5B%5B%22mapped_opening_hours%3A50300%3Cscore=2%3E%22%5D%2C%5B%22+is_online%3Atrue%3Cscore=1%3E%22%5D%5D&hitsPerPage=80&aroundLatLng=27.523647%2C41.696632&aroundRadius=all&aroundPrecision=1000&ruleContexts=%5B%22android%22%5D&distinct=1&getRankingInfo=true&clickAnalytics=true&analytics=true\"}],\"strategy\":\"none\"}"

        yield scrapy.Request(url=LinksSpider.url, body=payload, method="POST", headers=LinksSpider.headers, callback=self.parse, meta={'currentPage': 0})

    def parse(self, response):
        item = MrsoolLinks()
        jsonData = json.loads(response.text)
        
        hits = jsonData['results'][0]['hits'] #hits returns list with all restaurant data
        totalPages = jsonData['results'][0]['nbPages']
        
        geocodes = ['21.492500%2C39.177570', '18.329384%2C42.759365', '25.994478%2C45.318161', '26.094088%2C43.973454', '21.437273%2C40.512714', '29.953894%2C40.197044', '30.983334%2C41.016666', '24.186848%2C38.026428', '27.523647%2C41.696632', '24.774265%2C46.738586', '18.3%2C42.7333', '27.5236%2C41.7001', '26.5196%2C50.0115', '25.41%2C49.5808', '24.1556%2C47.312', '17.4917%2C44.1322', '24.0943%2C38.0493', '18.2167%2C42.5', '30.9833%2C41.0167', '16.8892%2C42.5611', '30%2C40.1333,' '20.0129%2C41.4677']
        
        if len(hits) > 0:
            for i in hits:
                shopID = i['shop_id']
                name = i['name']
                if '"' and "'" in name:
                    name = name.replace("'", "’")
                latitude = i['_geoloc']['lat']
                if latitude is None:
                    latitude = ""
                longitude = i['_geoloc']['lng']
                if longitude is None:
                    longitude = ""
                url = f'https://business-api.mrsool.co/v1/business_orders/refactored_new?latitude={latitude}&longitude={longitude}&shop_id={shopID}'

                item['shopID'] = shopID
                item['name'] = name
                item['latitude'] = latitude
                item['longitude'] = longitude
                item['url'] = url

                yield item

            for i in range(1, totalPages):
                payload = """{\"requests\":[{\"indexName\":\"services_production\",\"params\":\"query=&filters=%28+%22country_code%22%3A%22SA%22+AND+%22skip_distance_check%22%3A%22false%22+AND+%22mrsool_service%22%3A%22true%22+%29+AND+%28+promotion+%3E=+0+%29+AND+%28category_ids%3A166%29&optionalFilters=%5B%5B%22mapped_opening_hours%3A50300%3Cscore=2%3E%22%5D%2C%5B%22+is_online%3Atrue%3Cscore=1%3E%22%5D%5D&page=""" + str(i) + """&hitsPerPage=80&aroundLatLng=21.542086%2C39.15241&aroundRadius=all&aroundPrecision=1000&ruleContexts=%5B%22android%22%5D&distinct=1&getRankingInfo=true&clickAnalytics=true&analytics=true\"},{\"indexName\":\"services_production\",\"params\":\"query=&filters=%28+%22country_code%22%3A%22SA%22+AND+%22skip_distance_check%22%3A%22true%22+%29+AND+%28+promotion+%3E=+0+%29+AND+%28category_ids%3A166%29&optionalFilters=%5B%5B%22mapped_opening_hours%3A50300%3Cscore=2%3E%22%5D%2C%5B%22+is_online%3Atrue%3Cscore=1%3E%22%5D%5D&hitsPerPage=80&aroundLatLng=21.542086%2C39.15241&aroundRadius=all&aroundPrecision=1000&ruleContexts=%5B%22android%22%5D&distinct=1&getRankingInfo=true&clickAnalytics=true&analytics=true\"}],\"strategy\":\"none\"}"""

                yield scrapy.Request(url=LinksSpider.url, headers=LinksSpider.headers, body=payload, method="POST", callback=self.parse)
            
        for i in geocodes:
            for j in range(1, totalPages):
                payload = """{\"requests\":[{\"indexName\":\"services_production\",\"params\":\"query=&filters=%28+%22country_code%22%3A%22SA%22+AND+%22skip_distance_check%22%3A%22false%22+AND+%22mrsool_service%22%3A%22true%22+%29+AND+%28+promotion+%3E=+0+%29+AND+%28category_ids%3A77%29&optionalFilters=%5B%5B%22mapped_opening_hours%3A30930%3Cscore=2%3E%22%5D%2C%5B%22+is_online%3Afalse%3Cscore=1%3E%22%5D%5D&page=""" +str(j)+ """&hitsPerPage=80&aroundLatLng=""" + str(i) + """&aroundRadius=all&ruleContexts=%5B%22android%22%5D&distinct=1&getRankingInfo=true&clickAnalytics=true&analytics=true\"},{\"indexName\":\"services_production\",\"params\":\"query=&filters=%28+%22country_code%22%3A%22SA%22+AND+%22skip_distance_check%22%3A%22true%22+%29+AND+%28+promotion+%3E=+0+%29+AND+%28category_ids%3A77%29&optionalFilters=%5B%5B%22mapped_opening_hours%3A30930%3Cscore=2%3E%22%5D%2C%5B%22+is_online%3Atrue%3Cscore=1%3E%22%5D%5D&hitsPerPage=1&aroundLatLng=""" + str(i) + """&aroundRadius=all&ruleContexts=%5B%22android%22%5D&distinct=1&getRankingInfo=true&clickAnalytics=true&analytics=true\"}],\"strategy\":\"none\"}"""

                yield scrapy.Request(url=LinksSpider.url, headers=LinksSpider.headers, body=payload, method="POST", callback=self.parse)