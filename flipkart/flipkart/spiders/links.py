import scrapy, json
from scrapy.cmdline import execute
from flipkart.items import FlipkartLinks

class MobilesSpider(scrapy.Spider):
    name = 'links'
    url = 'https://2.rome.api.flipkart.com/api/4/page/fetch'
    headers = {
                'Accept': ' */*',
                'Accept-Encoding': ' gzip, deflate, br',
                'Accept-Language': ' en-US,en;q=0.6',
                'Connection': ' keep-alive',
                'Content-Type': ' application/json',
                'Cookie': ' T=TI166236960587300047271663263982592227857345080486809720377689484142; AMCVS_17EB401053DAF4840A490D4C%40AdobeOrg=1; AMCV_17EB401053DAF4840A490D4C%40AdobeOrg=-227196251%7CMCIDTS%7C19243%7CMCMID%7C38242552423001486423596715429429830488%7CMCAID%7CNONE%7CMCOPTOUT-1662552498s%7CNONE; s_sq=flipkart-prd%3D%2526pid%253Dwww.flipkart.com%25253Amobile-phones-store%2526pidt%253D1%2526oid%253Dhttps%25253A%25252F%25252Fwww.flipkart.com%25252Fsearch%25253Fp%2525255B%2525255D%25253Dfacets.brand%252525255B%252525255D%2525253DSamsung%252526sid%25253Dtyy%2525252F4io%252526sort%25253Drecency_%2526ot%253DA; S=d1t16PxQ/P0s/P1Y/PwIESTI/P5sZc1cufAuNVri1q9Hn/kIYMkd38zPTMi9jfHUTE4nUHHlmy9amxStOpUWoNfY1uw==; SN=VI17C06F7751D8455DB654940EDCB69B33.TOKE8E1E6591D8A4D8F84742AA1C2133E11.1662546035.LI; S=d1t16N3J+Fz8/BzE/P0I/Pz8/P6UgqKNGZGouYreVnQfJhuIUOlHpFaRIEcrTJWTG7FRQuwA5u5nrP4rTVm1l5JS0fQ==; SN=VI17C06F7751D8455DB654940EDCB69B33.TOKE8E1E6591D8A4D8F84742AA1C2133E11.1662550886.LI',
                'Host': ' 2.rome.api.flipkart.com',
                'Origin': ' https://www.flipkart.com',
                'Referer': ' https://www.flipkart.com/',
                'Sec-Fetch-Dest': ' empty',
                'Sec-Fetch-Mode': ' cors',
                'Sec-Fetch-Site': ' same-site',
                'Sec-GPC': ' 1',
                'User-Agent': ' Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
                'X-User-Agent': ' Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 FKUA/website/42/website/Desktop'
                }

    def start_requests(self):

        # pages = RESPONSE.slots[3].widget.data.filters.facetResponse.facets[1].values[1].values[21].count
        payload = "{\"pageUri\":\"/search?p%5B%5D=facets.brand%255B%255D%3DSamsung&sid=tyy%2F4io&sort=recency_desc&ctx=eyJjYXJkQ29udGV4dCI6eyJhdHRyaWJ1dGVzIjp7InRpdGxlIjp7Im11bHRpVmFsdWVkQXR0cmlidXRlIjp7ImtleSI6InRpdGxlIiwiaW5mZXJlbmNlVHlwZSI6IlRJVExFIiwidmFsdWVzIjpbIkxhdGVzdCBTYW1zdW5nIG1vYmlsZXMgIl0sInZhbHVlVHlwZSI6Ik1VTFRJX1ZBTFVFRCJ9fX19fQ%3D%3D&wid=1.productCard.PMU_V2_1\",\"pageContext\":{\"fetchSeoData\":true,\"paginatedFetch\":false,\"pageNumber\":1},\"requestContext\":{\"type\":\"BROWSE_PAGE\",\"ssid\":\"grn1m343o00000001662546038728\",\"sqid\":\"zl21u3e0ow0000001662546038728\"}}"

        yield scrapy.Request(url=MobilesSpider.url, headers=MobilesSpider.headers, body=payload, method="POST", meta={'currentPage': 1}, callback=self.parse)

    def parse(self, response):
        item = FlipkartLinks()
        jsonData = json.loads(response.text)
        currentPage = response.meta['currentPage']

        # JsonPath = RESPONSE.slots[8].widget.data.products[0].productInfo.value.smartUrl
        for i in jsonData['RESPONSE']['slots']:
            try:
                if 'PRODUCT_SUMMARY' in i['elementId']:
                    item['url'] = i['widget']['data']['products'][0]['productInfo']['value']['smartUrl'].rpartition('?')[0]
                    yield item
            except:
                pass
        
        for i in jsonData['RESPONSE']['slots']:
            try:
                if isinstance(i['widget']['data']['totalPages'], int):
                    totalpages = i['widget']['data']['totalPages']
            except:
                pass
            
        if currentPage < totalpages:
            currentPage += 1
            
            payload = """{\"pageUri\":\"/search?p%5B%5D=facets.brand%255B%255D%3DSamsung&sid=tyy%2F4io&sort=recency_desc&ctx=eyJjYXJkQ29udGV4dCI6eyJhdHRyaWJ1dGVzIjp7InRpdGxlIjp7Im11bHRpVmFsdWVkQXR0cmlidXRlIjp7ImtleSI6InRpdGxlIiwiaW5mZXJlbmNlVHlwZSI6IlRJVExFIiwidmFsdWVzIjpbIkxhdGVzdCBTYW1zdW5nIG1vYmlsZXMgIl0sInZhbHVlVHlwZSI6Ik1VTFRJX1ZBTFVFRCJ9fX19fQ%3D%3D&wid=1.productCard.PMU_V2_1\",\"pageContext\":{\"fetchSeoData\":true,\"paginatedFetch\":false,\"pageNumber\":"""+str(currentPage)+"""},\"requestContext\":{\"type\":\"BROWSE_PAGE\",\"ssid\":\"grn1m343o00000001662546038728\",\"sqid\":\"zl21u3e0ow0000001662546038728\"}}"""
            
            yield scrapy.Request(url=MobilesSpider.url, headers=MobilesSpider.headers, body=payload, method="POST", meta={'currentPage': currentPage}, callback=self.parse)


# if __name__ == '__main__':
#     execute('scrapy crawl links'.split())