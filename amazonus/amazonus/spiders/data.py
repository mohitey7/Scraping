import os, json
import unicodedata
from datetime import datetime

import scrapy
from scrapy.cmdline import execute
from scrapy.utils.response import open_in_browser

from amazonus.dbconfig import *
from amazonus.items import AmazonusData


class DataSpider(scrapy.Spider):
    name = 'data'
    links = ConfigDatabase(table='links', database='amazonus')

    def start_requests(self):
        cookies = {
    'session-id': '145-7780348-7954028',
    'session-id-time': '2082787201l',
    'i18n-prefs': 'USD',
    'ubid-main': '134-2853243-1253417',
    'csm-hit': 'tb:E1WBHA7946G6ZYKEJ8MV+s-E1WBHA7946G6ZYKEJ8MV|1665733266802&t:1665733266802&adb:adblk_no',
    'session-token': 'UWV4zNNC0sZsI8rOukaEZUcMDYiSYuEjByTIAyopHbA1qmmw3RCop+wJ4uwPb77XPii2CIIw42mGkmlIn0Sd8GIiP6CBbRpRnR6b8sS4GjIDde3NHgJEbattKCtaeJzcUgLANs9KTPQwv6FiPkVQt+EKBh7UKj7wQ7scG5/I+f7bJBXKtfbgnQOisCVBR4s6QPp40Z1Y7UJLsaAgl3eHgGZLl0S1hO4K',
}

        headers = {
            'authority': 'www.amazon.com',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            # Requests sorts cookies= alphabetically
            # 'cookie': 'session-id=145-7780348-7954028; session-id-time=2082787201l; i18n-prefs=USD; ubid-main=134-2853243-1253417; csm-hit=tb:E1WBHA7946G6ZYKEJ8MV+s-E1WBHA7946G6ZYKEJ8MV|1665733266802&t:1665733266802&adb:adblk_no; session-token=UWV4zNNC0sZsI8rOukaEZUcMDYiSYuEjByTIAyopHbA1qmmw3RCop+wJ4uwPb77XPii2CIIw42mGkmlIn0Sd8GIiP6CBbRpRnR6b8sS4GjIDde3NHgJEbattKCtaeJzcUgLANs9KTPQwv6FiPkVQt+EKBh7UKj7wQ7scG5/I+f7bJBXKtfbgnQOisCVBR4s6QPp40Z1Y7UJLsaAgl3eHgGZLl0S1hO4K',
            'referer': 'https://www.amazon.com/dp/B0B6228XBM?th=1',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'sec-gpc': '1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        }

        for i in DataSpider.links.fetchResultsfromSql():
            # url = i['URL']
            # yield scrapy.Request(url=url, callback=self.parse, meta={'ASIN':i['ASIN'], 'URL':i['URL']}, cookies=cookies, headers=headers)
            try:
                yield scrapy.Request(url=f"file:\\\\D:\Mohit Aswani\\amazonus\\AmazonPageData\\{i['ASIN']}.html", callback=self.parse, meta={'ASIN':i['ASIN']})
                # yield scrapy.Request(url=f"file:\\\\D:\Mohit Aswani\\amazonus\\AmazonPageData\\B083F3JKNN.html", callback=self.parse, meta={'ASIN':i['ASIN']})
            except Exception as e:
                print(f'{e} in html file!')
        # yield scrapy.Request(url=f"file:\\\\D:\Mohit Aswani\\amazonus\\AmazonPageData\\B07T71FTGB.html", callback=self.parse)

    def parse(self, response):
        captchatest = response.xpath('//div[@class="a-box a-alert a-alert-info a-spacing-base"]//h4/text()').get()
        if captchatest and 'characters you see below' in captchatest:
            print(f"Captcha caught in {response.url}")
        
        else:
            item = AmazonusData()

            # item['product_url'] = response.meta['URL']

            if response.xpath('//*[@id="productTitle"]/text()').get():
                item['product_name'] = response.xpath('//*[@id="productTitle"]/text()').get().strip()
            elif response.xpath('//span[@id="gc-asin-title"]/text()').get():
                item['product_name'] = response.xpath('//span[@id="gc-asin-title"]/text()').get().strip()
            else:
                item['product_name'] = ''

            availability = response.xpath('//div[@id="availability"]/span/text()').get()
            if availability and 'In Stock' in availability:
                item['is_sold_out'] = 'false'
            elif availability and ('Out of Stock' in availability or 'Currently' in availability):
                item['is_sold_out'] = 'true'
            elif response.xpath('//span[@id="submit.add-to-cart-announce"]').get() and 'Add to Cart' in response.xpath('//span[@id="submit.add-to-cart-announce"]').get():
                item['is_sold_out'] = 'false'
            elif response.xpath('//div[@id="gc-buy-box-atc-button"]//span[@class="a-button-text a-text-center"]/text()').get() and 'Add to cart' in response.xpath('//div[@id="gc-buy-box-atc-button"]//span[@class="a-button-text a-text-center"]/text()').get():
                item['is_sold_out'] = 'false'
            elif response.xpath('//span[@id="submit.buy-now-announce"]/text()').get() and 'Pre-order' in response.xpath('//span[@id="submit.buy-now-announce"]/text()').get():
                item['is_sold_out'] = 'false'
            elif response.xpath('//span[@id="buybox-see-all-buying-choices"]/span//text()').get() and 'See All Buying Options' in response.xpath('//span[@id="buybox-see-all-buying-choices"]/span//text()').get():
                item['is_sold_out'] = 'true'
            elif response.xpath('//div[@id="availability-string"]/span/text()').get() and 'In Stock' in response.xpath('//div[@id="availability-string"]/span/text()').get():
                item['is_sold_out'] = 'false'
            else:
                item['is_sold_out'] = 'true'

            if item['is_sold_out'] == 'false':

                # Discount ----------------------------------------------------------------------------------------------------------------------------
                
                discount = response.xpath('//div[@class="a-section a-spacing-none aok-align-center"]/span[@class="a-size-large a-color-price savingPriceOverride aok-align-center reinventPriceSavingsPercentageMargin savingsPercentage"]/text()').get()
                if discount:
                    item['discount'] = discount.replace('-', '').replace('%', '')
                
                if 'discount' not in item.keys():
                    discount = response.xpath('//span[@id="savingsPercentage"]/text()').get()
                    if discount and '%' in discount:
                        item['discount'] = discount.replace('%', '').replace('(', '').replace(')', '')

                if 'discount' not in item.keys():
                    discount = response.xpath('//tr[@id="regularprice_savings"]/td[contains(text(), "%")]/text()').get()
                    if discount and '%' in discount:
                        item['discount'] = discount.split('(')[1].split('%')[0].split()

                if 'discount' not in item.keys():
                    discount = response.xpath('//div[@id="corePrice_desktop"]//span[@class="a-color-price"]/text()').getall()
                    if len(discount) > 0:
                        for i in discount:
                            if '%' in i:
                                item['discount'] = i.replace('%', '').replace('(', '').replace(')', '').strip()
                                break
                
                if 'discount' not in item.keys():
                    discount = response.xpath('//div/div[@class="a-column a-span12 a-text-right"]/span[contains(text(), "Save")]/text()').get()
                    if discount and '%' in discount:
                        discount = discount
                        try:
                            if '(' in discount and '%' in discount:
                                item['discount'] = discount.split('(')[1].split('%')[0]
                        except:
                            item['discount'] = 'N/A'    
                    else:
                        item['discount'] = 'N/A'

                if isinstance(item['discount'], list):
                    item['discount'] = ''.join(item['discount'])
                    
                # Product Price ---------------------------------------------------------------------------------------------------

                price = response.xpath('//span[@id="priceblock_ourprice"]/span/text()').get()
                if price and '$' in price:
                    item['product_price'] = price.strip().replace('$', '')

                if 'product_price' not in item.keys():
                    price = response.xpath('//span[@id="price"]/text()').get()
                    if price and '$' in price:
                        item['product_price'] = price.strip().replace('$', '')

                if 'product_price' not in item.keys():
                    price = response.xpath('//span[@id="priceblock_pospromoprice"]/text()').get()
                    if price and '$' in price:
                        item['product_price'] = price.strip().replace('$', '')

                if 'product_price' not in item.keys():
                    price = response.xpath('//span[@id="priceblock_ourprice"]/text()').get()
                    if price and '$' in price:
                        item['product_price'] = price.strip().replace('$', '')

                if 'product_price' not in item.keys():
                    price = response.xpath('//div[@id="corePrice_desktop"]//span[@class="a-price a-text-price a-size-medium apexPriceToPay"]/span[@class="a-offscreen"]/text()').get()
                    if price and '$' in price:
                        item['product_price'] = price.strip().replace('$', '')

                if 'product_price' not in item.keys():
                    price = response.xpath('//table[@class="a-lineitem a-align-top"]//span[@class="a-offscreen"]/text()').getall()
                    if len(price) > 0:
                        price = []
                        for i in price:
                            if '$' in i:
                                price.append(i.strip('$'))
                        price.sort()
                        item['product_price'] = price[0]

                if 'product_price' not in item.keys():
                    price = response.xpath('//div[@id="dbsUnifiedAccordion"]//span[@class="a-size-medium a-color-price a-text-normal"]/text()').get()
                    if price and '$' in price:
                        item['product_price'] = price.strip().replace('$', '')

                if 'product_price' not in item.keys():
                    price = response.xpath('//div[@id="dbsUnifiedBuybox"]//span[@class="a-size-medium a-color-price a-text-normal"]/text()').get()
                    if price and '$' in price:
                        item['product_price'] = price.strip().replace('$', '')
                
                if 'product_price' not in item.keys():
                    price = response.xpath('//span[@id="priceblock_ourprice"]/text()').get()
                    if price and '$' in price:
                        item['product_price'] = price.strip().replace('$', '')

                if 'product_price' not in item.keys():
                    product_price = response.xpath('//div[@id="corePriceDisplay_desktop_feature_div"]//span[@data-a-size="xl"]/span[@class="a-offscreen"]/text()').get()
                    if product_price and '%' in product_price:
                        item['product_price'] = '0'
                    elif product_price:
                        item['product_price'] = product_price.strip().replace('$', '')
                    
                if 'product_price' not in item.keys():
                    product_price = response.xpath('//span[@id="snsDetailPagePrice"]/span[@id="sns-base-price"]/text()').get()
                    if product_price:
                        item['product_price'] = product_price.strip().replace('$', '')
                
                if 'product_price' not in item.keys():
                    product_price = response.xpath('//span[@class="a-button a-button-selected a-button-toggle"]//button[@id="gc-mini-picker-amount-1"]/@value').get()
                    if product_price and product_price[0].isnumeric():
                        item['product_price'] = product_price.strip('$')

                if 'product_price' not in item.keys():
                    product_price = response.xpath('//div[@id="corePrice_feature_div"]//span[@class="a-offscreen"]/text()').get()
                    if price and '$' in price:
                        item['product_price'] = price.strip().replace('$', '')

                if 'product_price' not in item.keys() or not item['product_price']:
                    item['product_price'] = '0'
                
                    

                # MRP ------------------------------------------------------------------------------------------------------------------------

                mrp = response.xpath('//div[@id="corePriceDisplay_desktop_feature_div"]//span[contains(text(), "Was")]//span[@class="a-offscreen"]/text()').get()
                if mrp and '$' in mrp:
                    item['mrp'] = mrp.replace('$', '')
                
                if 'mrp' not in item.keys():
                    mrp = response.xpath('//div[@id="price"]//td[@class="a-span12 a-color-secondary a-size-base"]/span[@class="priceBlockStrikePriceString a-text-strike"]/text()').get()
                    if mrp and '$' in mrp:
                        item['mrp'] = mrp.replace('$', '')

                if 'mrp' not in item.keys():
                    mrp = response.xpath('//span[@id="listPrice"]/text()').get()
                    if mrp and '$' in mrp:
                        item['mrp'] = mrp.replace('$', '')

                if 'mrp' not in item.keys():
                    mrp = response.xpath('//div[@id="corePrice_desktop"]//tr/td[contains(text(), "List Price")]/following-sibling::td//span[@class="a-offscreen"]/text()').get()
                    if mrp and '$' in mrp:
                        item['mrp'] = mrp.replace('$', '')
                        
                if 'mrp' not in item.keys():
                    mrp = response.xpath('//div[@id="dbsUnifiedAccordion"]//span[@class="a-size-small a-color-tertiary a-text-strike"]/text()').get()
                    if mrp and '$' in mrp:
                        item['mrp'] = mrp.replace('$', '').strip()

                if 'mrp' not in item.keys():
                    mrp = response.xpath('//div[@id="corePriceDisplay_desktop_feature_div"]//span[contains(text(), "List Price")]//span[@class="a-offscreen"]/text()').get()
                    if mrp and '$' in mrp:
                        item['mrp'] = mrp.replace('$', '')
                
                if 'mrp' not in item.keys():
                    mrp = response.xpath('//div[@id="corePrice_desktop"]//span[@class="a-size-base a-color-secondary"]/text()').get()
                    if mrp and '$' in mrp:
                        item['mrp'] = mrp.replace('$', '')
                
                if 'mrp' not in item.keys() or not item['mrp']:
                    item['mrp'] = item['product_price']


                    
                # Deal of the day -------------------------------------------------------------------------------------------------
            
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

                arrivaldate = response.xpath('//div[@id="mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE"]/span/@data-csa-c-delivery-time').get()
                if arrivaldate:
                    item['arrival_date'] = arrivaldate
                
                
                shipping_charge = response.xpath('//div[@id="deliveryBlockMessage"]//span[@data-csa-c-delivery-type="delivery"]/@data-csa-c-delivery-price').getall()
                if len(shipping_charge) > 0:
                    for i in shipping_charge:
                        if 'FREE' in i.strip() or 'Free' in i.strip():
                            item['shipping_charge'] = '0'
                            break

                if 'shipping_charge' not in item.keys():
                    shipping_charge = response.xpath('//div[@class="olp-text-box"]//span[contains(text(), "shipping")]/text()').get()
                    if shipping_charge and 'shipping' in shipping_charge:
                        item['shipping_charge'] = shipping_charge.strip(' shipping').strip('+ $')

                if 'shipping_charge' not in item.keys():
                    shipping_charge = response.xpath('//div[@id="deliveryBlockMessage"]//span[@data-csa-c-delivery-type="delivery"]/@data-csa-c-delivery-price').get()
                    if shipping_charge and '$' in shipping_charge:
                        item['shipping_charge'] = shipping_charge.replace('$', '')

                if 'shipping_charge' not in item.keys():
                    item['shipping_charge'] = '0'

                coupon = response.xpath('//div[@id="promoPriceBlockMessage_feature_div"]//label[contains(text(), "coupon")]/text()').get()
                if coupon and 'coupon' in coupon:
                    item['coupon'] = coupon.strip()

            elif item['is_sold_out'] == 'true':
                item['discount'] = 'N/A'
                item['product_price'] = item['mrp'] = '0'
                item['shipping_charge'] = '0'
                item['deal_of_the_day'] = item['deal_price'] = 'false'

            # elif response.xpath('//div[@id="corePrice_desktop"]//span[@aria-hidden="true"]/text()').get() and '$' in response.xpath('//div[@id="corePrice_desktop"]//span[@aria-hidden="true"]/text()').get():
            #     item['mrp'] = response.xpath('//div[@id="corePrice_desktop"]//span[@aria-hidden="true"]/text()').get().replace('$', '')
            
            if response.xpath('//div[@id="glow-ingress-block"]/span[@id="glow-ingress-line2"]/text()').get():
                item['city'] = response.xpath('//div[@id="glow-ingress-block"]/span[@id="glow-ingress-line2"]/text()').get().strip().encode("ascii", "ignore").decode()
            elif response.xpath('//*[@id="contextualIngressPtLabel_deliveryShortLine"]/span[2]/text()').get():
                item['city'] = response.xpath('//*[@id="contextualIngressPtLabel_deliveryShortLine"]/span[2]/text()').get().strip().encode("ascii", "ignore").decode()

            images = response.xpath('//span[@aria-hidden="true"]/img/@src').getall()
            if len(images) > 0:
                for i in range(len(images)):
                    images[i] = images[i].rsplit('.', 2)[0] + '.' + images[i].rsplit('.', 2)[-1]
                item['image_urls'] = ' | '.join([i for i in images if '.gif' not in i])
            
            if 'image_urls' not in item.keys():
                images = response.xpath('//div[@id="imageBlockThumbs"]//img/@src').getall()
                if len(images) > 0:
                    for i in range(len(images)):
                        images[i] = images[i].rsplit('.', 2)[0] + '.' + images[i].rsplit('.', 2)[-1]
                    item['image_urls'] = ' | '.join([i for i in images if '.gif' not in i])
            
            if 'image_urls' not in item.keys():
                if response.xpath('//img[@id="gc-standard-design-image"]/@src').get() and 'media' in response.xpath('//img[@id="gc-standard-design-image"]/@src').get():
                    item['image_urls'] = response.xpath('//img[@id="gc-standard-design-image"]/@src').get().strip()

            item['category_hierarchy'] = []
            categorylist = response.xpath('//ul[@class="a-unordered-list a-horizontal a-size-small"]/li/span[@class="a-list-item"]/a/text()').getall()
            for i in categorylist:
                item['category_hierarchy'].append(i.strip())
            item['category_hierarchy'] = ' > '.join(item['category_hierarchy'])

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
                item['about_item'] = ' | '.join([i for i in about_item if i != ''])

            elif len(response.xpath('//li[@class="a-spacing-small"]//text()').getall()) > 0:
                about_item = response.xpath('//li[@class="a-spacing-small"]//text()').getall()
                for i in range(len(about_item)):
                    about_item[i] = unicodedata.normalize("NFKD", about_item[i].strip())
                item['about_item'] = ' | '.join([i for i in about_item if i != ''])

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
                item['product_detail'] = json.dumps(product_detail)

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
                item['product_specification'] = json.dumps(product_specification)
            
            if "best_seller_rank" not in item.keys():
                bestsellersrank = response.xpath('//*[@id="detailBulletsWrapper_feature_div"]/ul[1]/li/span//text()').getall()
                for i in range(len(bestsellersrank)):
                    bestsellersrank[i] = bestsellersrank[i].strip()
                if len(bestsellersrank) > 0:
                    bestsellersrank = ' '.join(i for i in bestsellersrank if (i!= '' and 'Best' not in i)).strip('#')
                    bestsellersrank = bestsellersrank.replace(' #', ' | #')
                    item['best_seller_rank'] = '#' + bestsellersrank

            if 'product_id' not in item.keys():
                item['product_id'] = response.meta['ASIN']

            item['product_url'] = 'https://www.amazon.com/dp/' + item['product_id']
            
            # path = f'{os.getcwd()}\\AmazonPageData\\'
            # if not os.path.exists(path):
            #     os.makedirs(path)
            # with open(f"{path}{item['product_id']}.html", 'wb') as f:
            #     f.write(response.body)

            item['scrapped_at'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            
            yield item

if __name__ == '__main__':
    execute('scrapy crawl data'.split())