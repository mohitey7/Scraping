import scrapy
from ..items import AmazonAuLinks


class LinksSpider(scrapy.Spider):
    name = 'links'
    headers = {
            'authority': 'www.amazon.com.au',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-IN,en;q=0.8',
            'cache-control': 'max-age=0',
            # Requests sorts cookies= alphabetically
            # 'cookie': 'session-id=357-3121646-9551328; i18n-prefs=AUD; ubid-acbau=355-0388732-8322043; session-token="zdolGY28aMrSsQJdRoyh5oX9oc0Pvm3nJzWkkvsXy29mauRQylFYJyG0oPjDOvCTq6EPSqPxvbLITHW0kpuMzNdrK4IFdtTictAjod4MntlDMeRLmch7kvtmOQ6C3wIJd3r2vOYS3jwbYKAwbYWIMhvAeUTts3Y85cv8mpLDswBGCU3vyi/63LKcpF7eRraGZBJJDUXZ9OCN3400j1u+tqQG9BvlaePKCGEzQ7EddM0="; session-id-time=2082758401l; csm-hit=tb:H7SE74GJJTREJD0TMAAH+s-N8761N640V4A13V1MWQX|1664711484431&t:1664711484431&adb:adblk_no',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'sec-gpc': '1',
            'service-worker-navigation-preload': 'true',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
            }
    
    def start_requests(self):
        for i in range(1, 5):
            url = f'https://www.amazon.com.au/s?k=Natural+Rich+Linen+Curtains+Semi+Sheer+for+Bedroom%2FLiving+Room&crid=23D05QIKYZUNX&sprefix=natural+rich+linen+curtains+semi+sheer+for+bedroom%2Fliving+room%2Caps%2C359&page={i}'

            yield scrapy.Request(url=url, headers=LinksSpider.headers, callback=self.parse)

    def parse(self, response):
        if response.status == 200:
            item = AmazonAuLinks()
            links = response.xpath('//a[@class="a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal"]/@href').getall()
            
            for link in links:
                item['URL'] = 'https://www.amazon.com.au' + link.split('/ref=')[0]
                yield item
        
        else:
            print("Response error!")