import scrapy, json, pymysql, os
from datetime import date
from ..items import SheinData

class DataSpider(scrapy.Spider):
    name = 'data'
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='shein')
    cursor = connection.cursor()

    def start_requests(self):
        DataSpider.cursor.execute("select * from links where Status = 'Pending' limit 4000")
        links = DataSpider.cursor.fetchall()
        
        # for link in links:
        #     yield scrapy.Request(url=link[1], callback=self.parse, meta={'url': link[1]})
        
        url = 'https://sg.shein.com/Butterfly-Embroidery-Tank-Top-&-Biker-Shorts-p-9934030-cat-1780.html'
        yield scrapy.Request(url=url, callback=self.parse, meta={'url': url})

    def parse(self, response):
        if response.status == 200:
            item = SheinData()
            rawJs = response.xpath('//script[contains(text(), "productIntroData")]').get()
            rawJs = rawJs.split('productIntroData: ')[1]
            rawJs = rawJs.split('abt: ')[0].replace(',\n        ', '')
            
            jsonData = json.loads(rawJs)

            item['product_name'] = jsonData['detail']['goods_name']
            item['product_id'] = jsonData['detail']['goods_sn']

            path = f'{os.getcwd()}\\{date}\\'
            if not os.path.exists(path):
                os.makedirs(path)
            with open(f"{path}{item['product_id']}.html", 'wb') as f:
                f.write(response.body)
            
            images = []
            mainImage = jsonData['goods_imgs']['main_image']['origin_image']
            if mainImage:
                mainImage = 'https:' + mainImage
                images.append(mainImage)
            
            if jsonData['goods_imgs']['detail_image']:
                for i in jsonData['goods_imgs']['detail_image']:
                    images.append('https:' + i['origin_image'])
            
            if len(images) > 1:
                item['image_url'] = images[0]
            
            # detail.retailPrice.amount
            mrp = jsonData['detail']['retailPrice']['amount']
            
            # detail.salePrice.amount
            price = jsonData['detail']['salePrice']['amount']
            
            if float(price) > 40:
                item['shipping_charges'] = '0'
            else:
                item['shipping_charges'] = '1'

            goodsID = jsonData['multiLocalSize']['goods_id']
            try:
                for i in jsonData['attrSizeList']['sale_attr_list'][str(goodsID)]['sku_list']:
                    discount = i['price']['unit_discount']
                    if discount != '0':
                        item['discount'] = discount
                        break
            except Exception as e:
                print(e)

            try:
                item['number_of_ratings'] = jsonData['commentInfo']['comment_num']
            except Exception as e:
                print(f'{e} in reviews!')
            
            try:
                item['avg_rating'] = jsonData['commentInfo']['comment_rank_average']
            except Exception as e:
                print(f'{e} in ratings!')

            # detail.productDetails[0].attr_name
            others = dict()
            for i in jsonData['detail']['productDetails']:
                key = i['attr_name']
                value = [str(i['attr_value'])]
                if key in others.keys():
                    others[key].append(', '.join(value))
                else:
                    others[key] = value
            
            for i in others.keys():
                others[i] = ', '.join(others[i])
            
            others['image_URLs'] = images
            others['delivery'] = 'logout'
            others['data_vendor'] = 'Actowiz'
            
            item['others'] = json.dumps(others)
            
            
            # l2 = parentCats.children[0].cat_name
            # l3 = parentCats.children[0].children[0].cat_name
            # l4 = parentCats.children[0].children[0].children[0].cat_name
            
            category_hierarchy = dict()
            
            l1 = ''
            l2 = ''
            l3 = ''
            l4 = ''

            item['l1'] = jsonData['parentCats']['cat_name']
            if len(item['l1']) > 0:
                category_hierarchy['l1'] = item['l1']

            try:
                for i in jsonData['parentCats']['children']:
                    item['l2'] = i['cat_name']
                    if len(item['l2']) > 0:
                        category_hierarchy['l2'] = item['l2']
                    try:
                        for j in i['children']:
                            l3 = j['cat_name']
                            if len(l3) > 0:
                                category_hierarchy['l3'] = l3
                            try:
                                for k in j['children']:
                                    l4 = k['cat_name']
                                    if len(l4) > 0:
                                        category_hierarchy['l4'] = l4
                            except Exception as e:
                                l4 = ''
                                print(f'{e} in l4')
                    except Exception as e:
                        l3 = ''
                        print(f'{e} in l3')
            except Exception as e:
                item['l2'] = ''
                print(f'{e} in l2')
            
            item['category_hierarchy'] = json.dumps(category_hierarchy)

            # detail.mall_stock[0].stock
            try:
                for i in jsonData['detail']['mall_stock']:
                    if i['stock'] == 0:
                        item['is_sold_out'] = 'TRUE'
                        item['shipping_charges'] = '0'
                    else:
                        item['is_sold_out'] = 'FALSE'
            except Exception as e:
                print(f'{e} in Sold Out column')
                item['is_sold_out'] = 'FALSE'

            
            # attrSizeList.sale_attr_list[9934030].sku_list[0].stock
            # attrSizeList.sale_attr_list[9934030].sku_list[0].mall_stock[0].stock
            # item['is_sold_out'] = 'FALSE'
            # try:
            #     for i in jsonData['attrSizeList']['sale_attr_list'][str(goodsID)]['sku_list']:
            #         if i['stock'] == 0:
            #             item['is_sold_out'] = 'TRUE'
            #             break
            #         try:
            #             for j in i['mall_stock']:
            #                 if j['stock'] == 0:
            #                     item['is_sold_out'] = 'TRUE'
            #                     break
            #         except Exception as e:
            #             print(e)
            # except Exception as e:
            #     print(e)
            
            # if item['is_sold_out'] == 'TRUE':
            #     item['shipping_charges'] = '0'
            item['scraped_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            item['mrp'] = mrp
            item['product_price'] = price
            item['product_url'] = response.meta['url']
            item['page_url'] = response.meta['url']
            item['catalog_name'] = item['l1']
            item['l3'] = l3
            item['l4'] = l4

            # shippingUrl = "https://sg.shein.com/goods_detail_v2/getDelayWarn?_lang=en&_ver=1.1.8"
            # payload = f"goods_sn={item['product_id']}&countryId=191&business_model=0&mall_code=1"
            
            yield item
        
        else:
            print("Response Error")