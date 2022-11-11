# import scrapy, sys, json
# sys.path.append('../pedidosya')
# from scrapy.cmdline import execute
# from pedidosya.items import PedidosyaLinks
#
#
# from scraper_api import ScraperAPIClient
# client = ScraperAPIClient('4dc091f3df949e8efb2578b8aa21f725')
#
# class LinksSpider(scrapy.Spider):
#     name = 'links'
#     allowed_domains = ['test.in']
#     start_urls = ['http://test.in/']
#     def parse(self, response):
#
#         url ='https://www.google.com/search?q=site:https://www.pedidosya.cl/restaurantes+%22a%22&&start=100&num=300'
#         yield scrapy.Request(url=url,callback=self.)
# if __name__ == '__main__':
#     execute("scrapy crawl links".split())