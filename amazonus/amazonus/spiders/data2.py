import os
import unicodedata
from datetime import datetime

import scrapy
from scrapy.cmdline import execute
from scrapy.utils.response import open_in_browser

from amazonus.dbconfig import *
from amazonus.items import AmazonusData


class DataSpider(scrapy.Spider):
    name = 'data2'
    data = ConfigDatabase(table='data', database='amazonus')

    def start_requests(self):
        for i in DataSpider.data.fetchResultsfromSql():
            product_id = i['product_id']
            url = f"file:\\\\D:\Mohit Aswani\\amazonus\\AmazonPageData\\{product_id}.html"
            # url = f"file:\\\\D:\Mohit Aswani\\amazonus\\AmazonPageData\\B010IBV522.html"
            yield scrapy.Request(url=url, callback=self.parse, meta={'product_id':i['product_id'], 'product_url':i['product_url']})

    def parse(self, response):
        item = AmazonusData()

        item['product_url'] = response.meta['product_url']

        item['product_name'] = response.xpath('//*[@id="productTitle"]/text()').get()
        if item['product_name']:
            item['product_name'] = item['product_name'].strip()

        item['discount'] = response.xpath('//div[@class="a-section a-spacing-none aok-align-center"]/span[@class="a-size-large a-color-price savingPriceOverride aok-align-center reinventPriceSavingsPercentageMargin savingsPercentage"]/text()').get()
        if item['discount']:
            item['discount'] = item['discount'].replace('-', '').replace('%', '')
            pass
        else:
            item['discount'] = 'N/A'

        item['city'] = response.xpath('//*[@id="contextualIngressPtLabel_deliveryShortLine"]/span[2]/text()').get()

        product_price = response.xpath('//div[@id="corePriceDisplay_desktop_feature_div"]//span[@data-a-size="xl"]/span[@class="a-offscreen"]/text()').get()
        if product_price and '%' in product_price:
            item['product_price'] = '0'
        else:
            item['product_price'] = product_price

        if product_price is None:
            product_price = response.xpath('//span[@id="snsDetailPagePrice"]/span[@id="sns-base-price"]/text()').get()
            if product_price:
                item['product_price'] = product_price.strip().replace('$', '')

        if 'product_price' not in item.keys() or not item['product_price']:
            item['product_price'] = '0'
        
        mrp = response.xpath('//div[@id="corePriceDisplay_desktop_feature_div"]//span[contains(text(), "Was")]//span[@class="a-offscreen"]/text()').get()
        if mrp and '$' in mrp:
            item['mrp'] = mrp.replace('$', '')

        if 'mrp' not in item.keys():
            mrp = response.xpath('//div[@id="corePriceDisplay_desktop_feature_div"]//span[contains(text(), "List Price")]//span[@class="a-offscreen"]/text()').get()
            if mrp and '$' in mrp:
                item['mrp'] = mrp.replace('$', '')

        if 'mrp' not in item.keys() or not item['mrp']:
            item['mrp'] = item['product_price']
        # elif response.xpath('//div[@id="corePrice_desktop"]//span[@aria-hidden="true"]/text()').get() and '$' in response.xpath('//div[@id="corePrice_desktop"]//span[@aria-hidden="true"]/text()').get():
        #     item['mrp'] = response.xpath('//div[@id="corePrice_desktop"]//span[@aria-hidden="true"]/text()').get().replace('$', '')

        deal_of_the_day = response.xpath('//div[@id="corePrice_desktop"]//td[contains(text(), "With Deal")]/text()').get()
        if deal_of_the_day and 'With Deal' in deal_of_the_day:
            item['deal_of_the_day'] = 'true'
            item['deal_price'] = 'true'
            item['mrp'] = response.xpath('//div[@id="corePrice_desktop"]//td[@class="a-span12 a-color-secondary a-size-base"]//span[@class="a-offscreen"]/text()').get()
            product_price = response.xpath('//div[@id="corePrice_feature_div"]/span[@data-a-color="price"]/span[@class="a-offscreen"]/text()').get()
            if product_price and '$' in product_price:
                item['product_price'] = product_price
        else:
            item['deal_of_the_day'] = 'false'
            item['deal_price'] = 'false'

        images = response.xpath('//span[@aria-hidden="true"]/img/@src').getall()
        if len(images) > 0:
            for i in range(len(images)):
                images[i] = images[i].rsplit('.', 2)[0] + '.' + images[i].rsplit('.', 2)[-1]
            item['image_urls'] = ' | '.join([i for i in images if '.gif' not in i])

        item['category_hierarchy'] = []
        categorylist = response.xpath('//ul[@class="a-unordered-list a-horizontal a-size-small"]/li/span[@class="a-list-item"]/a/text()').getall()
        for i in categorylist:
            item['category_hierarchy'].append(i.strip())
        item['category_hierarchy'] = ' >> '.join(item['category_hierarchy'])

        item['number_of_ratings'] = response.xpath('//span[@id="acrCustomerReviewText"]/text()').get()
        if item['number_of_ratings'] and ' ratings' in item['number_of_ratings']:
            item['number_of_ratings'] = item['number_of_ratings'].split(' ')[0].replace(',', '')
        
        answered_questions = response.xpath('//a[@id="askATFLink"]/span/text()').get()
        if answered_questions:
            item['answered_questions'] = answered_questions.strip().split(' ')[0]
        
        item['avg_rating'] = response.xpath('//span[@id="acrPopover"]/@title').get()
        if item['avg_rating'] and 'stars' in item['avg_rating']:
            item['avg_rating'] = item['avg_rating'].split(' ')[0]

        about_item = response.xpath('//ul[@class="a-unordered-list a-vertical a-spacing-mini"]/li/span[@class="a-list-item"]/text()').getall()
        if len(about_item) > 0:
            for i in range(len(about_item)):
                about_item[i] = unicodedata.normalize("NFKD", about_item[i].strip())
            item['about_item'] = str([i for i in about_item if i != ''])

        elif len(response.xpath('//li[@class="a-spacing-small"]//text()').getall()) > 0:
            about_item = response.xpath('//li[@class="a-spacing-small"]//text()').getall()
            for i in range(len(about_item)):
                about_item[i] = unicodedata.normalize("NFKD", about_item[i].strip())
            item['about_item'] = str([i for i in about_item if i != ''])

        availability = response.xpath('//div[@id="availability"]/span/text()').get()
        if availability and ('In Stock' in availability or '.' in availability):
            item['is_sold_out'] = 'false'
        elif response.xpath('//span[@id="submit.add-to-cart-announce"]').get() and 'Add to Cart' in response.xpath('//span[@id="submit.add-to-cart-announce"]').get():
            item['is_sold_out'] = 'false'
        elif availability and 'Out of Stock' in availability:
            item['is_sold_out'] = 'true'
        elif response.xpath('//span[@id="buybox-see-all-buying-choices"]/span//text()').get() and 'See All Buying Options' in response.xpath('//span[@id="buybox-see-all-buying-choices"]/span//text()').get():
            item['is_sold_out'] = 'true'
        else:
            item['is_sold_out'] = 'true'

        item['shipping_charge'] = '0'

        if item['is_sold_out'] == 'false':
            arrivaldate = response.xpath('//div[@id="mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE"]/span/@data-csa-c-delivery-time').get()
            if arrivaldate:
                item['arrival_date'] = arrivaldate
            else:
                shipping_charge = response.xpath('//div[@class="olp-text-box"]//span[contains(text(), "shipping")]/text()').get()
                if shipping_charge and 'shipping' in shipping_charge:
                    item['shipping_charge'] = shipping_charge.strip(' shipping').strip('+ $')
            coupon = response.xpath('//div[@id="promoPriceBlockMessage_feature_div"]//label[contains(text(), "coupon")]/text()').get()
            if coupon and 'coupon' in coupon:
                item['coupon'] = coupon.strip()

        try:
            product_detail = dict()
            for i in response.xpath('//*[@id="detailBullets_feature_div"]//span[@class="a-list-item"]'):
                key = i.xpath('./span[1]/text()').get()
                if key:
                    key = key.split('\n')[0]
                value = i.xpath('./span[2]/text()').get()
                
                if key and value:
                    product_detail[key.strip()] = value.strip()
            
        except Exception as e:
            print(f'{e} in product detail block!')
        
        for i in response.xpath('//div[@class="a-section a-spacing-small a-spacing-top-small"]/table//tr'):
            key = i.xpath('.//td[1]/span//text()').get()
            value = i.xpath('.//td[2]/span//text()').get().replace('\t', '')
            if key and value and key.strip() not in product_detail.keys():
                product_detail[f'{key.strip()}'] = value.strip()

        if len(product_detail) > 0:
            item['product_detail'] = str(product_detail)

        soldby = response.xpath('//div[@tabular-attribute-name="Sold by"]//span//text()').getall()
        if len(soldby) == 2 and soldby[0] == 'Sold by':
            item['sold_by'] = soldby[1]

        soldbylink = response.xpath('//div[@tabular-attribute-name="Sold by"]//span//@href').get()
        if soldbylink:
            item['sold_by_link'] = 'https://www.amazon.com' + soldbylink

        shipsfrom = response.xpath('//div[@tabular-attribute-name="Ships from"]//span//text()').getall()
        if len(shipsfrom) == 2 and shipsfrom[0] == 'Ships from':
            item['ships_from'] = shipsfrom[1]
            if 'amazon' in item['ships_from']:
                item['ships_from_link'] = item['ships_from']
            elif 'sold_by_link' in item.keys() and item['sold_by'] in item['ships_from']:
                item['ships_from_link'] = item['sold_by_link']

        try:
            product_specification = dict()
            for i in response.xpath('//*[@id="productDetails_detailBullets_sections1"]//tr'):
                key = i.xpath('./th//text()').get()
                value = i.xpath('./td//text()').getall()
                
                try:
                    if key not in product_specification.keys() and 'Reviews' not in key and 'Best Sellers Rank' not in key:
                        if len(value) > 1:
                            product_specification[f'{key.strip()}'] = ', '.join(i for i in value if i.strip() != '').strip()
                        else:
                            product_specification[f'{key.strip()}'] = ''.join(i.replace('\t', '') for i in value if i.strip() != '').strip()

                    elif 'Best Sellers Rank' in key:
                        if len(value) > 1:
                            item['best_seller_rank'] = ''.join(i for i in value if i.strip() != '').split('#')
                            item['best_seller_rank'] = '#' + ' | #'.join(i for i in item['best_seller_rank'] if i.strip() != '')
                        else:
                            item['best_seller_rank'] = ''.join(i for i in value if i.strip() != '').strip()
                
                    elif 'ASIN' in key and 'product_id' not in item.keys():
                        if len(value) > 1:
                            item['product_id'] = ''.join(i for i in value if i.strip() != '')

                except Exception as e:
                    print(f'{e} in inner block of product specification!')        
                
        except Exception as e:
            print(f'{e} in product specification block!')

        try:
            technicaldetails = response.xpath('//*[@id="productDetails_techSpec_section_1"]//tr')
            for i in technicaldetails:
                key = i.xpath('./th//text()').get()
                value = i.xpath('./td//text()').getall()

                try:
                    if key and len(value)>0:
                        if key not in product_specification.keys() and 'Reviews' not in key and 'Best Sellers Rank' not in key and 'ASIN' not in key:
                            if len(value) > 1:
                                product_specification[f'{key.strip()}'] = ', '.join(i for i in value if i.strip() != '').strip()
                                product_specification[f'{key.strip()}'] = product_specification[f'{key.strip()}'].encode("ascii", "ignore").decode()
                            else:
                                product_specification[f'{key.strip()}'] = ''.join(i.replace('\t', '') for i in value if i.strip() != '').strip()
                                product_specification[f'{key.strip()}'] = product_specification[f'{key.strip()}'].encode("ascii", "ignore").decode()

                        elif 'Best Sellers Rank' in key and 'best_seller_rank' not in item.keys():
                            if len(value) > 1:
                                item['best_seller_rank'] = ''.join(i for i in value if i.strip() != '').split('#')
                                item['best_seller_rank'] = '#' + ' | #'.join(i for i in item['best_seller_rank'] if i.strip() != '')
                            else:
                                item['best_seller_rank'] = ''.join(i for i in value if i.strip() != '').strip()

                        elif 'ASIN' in key and 'product_id' not in item.keys():
                            if len(value) > 1:
                                item['product_id'] = ''.join(i for i in value if i.strip() != '')
                
                except Exception as e:
                    print(f'{e} in inner block of technical details!')
        
        except Exception as e:
            print(f'{e} in technical details block!')

        if len(product_specification) > 0:
            item['product_specification'] = str(product_specification)
        
        if "best_seller_rank" not in item.keys():
            bestsellersrank = response.xpath('//*[@id="detailBulletsWrapper_feature_div"]/ul[1]/li/span//text()').getall()
            for i in range(len(bestsellersrank)):
                bestsellersrank[i] = bestsellersrank[i].strip()
            if len(bestsellersrank) > 0:
                bestsellersrank = ' '.join(i for i in bestsellersrank if (i!= '' and 'Best' not in i)).strip('#')
                bestsellersrank = bestsellersrank.replace(' #', ' | #')
                item['best_seller_rank'] = '#' + bestsellersrank

        if 'product_id' not in item.keys():
            item['product_id'] = response.meta['product_id']

        # path = f'{os.getcwd()}\\AmazonPageData\\'
        # if not os.path.exists(path):
        #     os.makedirs(path)
        # with open(f"{path}{item['product_id']}.html", 'wb') as f:
        #     f.write(response.body)

        item['scrapped_at'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

        yield item

if __name__ == '__main__':
    execute('scrapy crawl data'.split())