import scrapy, dateparser
from itemloaders.processors import MapCompose, Join
from re import sub

def modifies_image_urls(value: str):
    if value:
        image_split = value.split('.')
        if len(image_split) > 4:
            image_url = '.'.join(image_split[:-2])
            return image_url + '.' + image_split[-1]
        else:
            return value

def clean_name(value: str):
    if value.strip():
        value = (value.strip().replace('\\', '').replace('"', '').replace("\u200c", "").replace("\u200f", "").replace("\u200e", "").replace("\n", " ").replace("\r", " "))
        if "\n" in value:
            value = " ".join(value.split())
        return value

def clear_price(value: str):
    if value.strip():
        return sub(r'[^0-9.]', '', value).strip()

def process_arrival_date(value: str):
    if value:
        return str(dateparser.parse(value.split("-")[0]))

def shipping_charges(value):
    if 'FREE delivery' not in value:
        return value

class SheinLinks(scrapy.Item):
    def __setitem__(self, key, value):
        if key not in self.fields:
            self.fields[key] = scrapy.Field()
        self._values[key] = value
        super().__setitem__(key, value)

class SheinData(scrapy.Item):
    input_pid = scrapy.Field()
    product_id = scrapy.Field()
    catalog_name = scrapy.Field()
    catalog_id = scrapy.Field(input_processor=MapCompose(clean_name, str.split), output_processor=Join())
    source = scrapy.Field()
    scraped_date = scrapy.Field()
    product_name = scrapy.Field()
    category_hierarchy = scrapy.Field()
    product_price = scrapy.Field(input_processor=MapCompose(clear_price, float))
    shipping_charges = scrapy.Field(input_processor=MapCompose(shipping_charges, clear_price, float))
    is_sold_out = scrapy.Field()
    discount = scrapy.Field()
    mrp = scrapy.Field()
    page_url = scrapy.Field()
    product_url = scrapy.Field()
    number_of_ratings = scrapy.Field()
    avg_rating = scrapy.Field()
    position = scrapy.Field()
    country_code = scrapy.Field()
    others = scrapy.Field()
    status = scrapy.Field()
    l1 = scrapy.Field()
    l2 = scrapy.Field()
    l3 = scrapy.Field()
    l4 = scrapy.Field()
    image_url = scrapy.Field()

# class ShopsyItem(scrapy.Item):
#     product_id = scrapy.Field()
#     input_pid = scrapy.Field()
#     catalog_name = scrapy.Field(input_processor=MapCompose(clean_name, str.split), output_processor=Join())
#     catalog_id = scrapy.Field()
#     source = scrapy.Field()
#     scraped_date = scrapy.Field()
#     product_name = scrapy.Field()
#     image_url = scrapy.Field(input_processor=MapCompose(modifies_image_urls))
#     category_hierarchy = scrapy.Field()
#     product_price = scrapy.Field(input_processor=MapCompose(clear_price, float))
    # arrival_date = scrapy.Field(input_processor=MapCompose(process_arrival_date))
#     shipping_charges = scrapy.Field(input_processor=MapCompose(shipping_charges, clear_price, float))
#     is_sold_out = scrapy.Field()
#     discount = scrapy.Field()
#     mrp = scrapy.Field(input_processor=MapCompose(clear_price, float))
#     product_url = scrapy.Field()
#     number_of_ratings = scrapy.Field()
#     avg_rating = scrapy.Field()
#     others = scrapy.Field()
#     is_zip = scrapy.Field()
#     Id = scrapy.Field()
#     page_url = scrapy.Field()
#     zip_code = scrapy.Field()
#     is_login = scrapy.Field()
#     country_code = scrapy.Field()
#     shipping_charges_json = scrapy.Field()
#     product_price_json = scrapy.Field()
#     mrp_json = scrapy.Field()
#     discount_json = scrapy.Field()
#     status = scrapy.Field()
#     l1 = scrapy.Field()
#     l2 = scrapy.Field()
#     islogin = scrapy.Field()