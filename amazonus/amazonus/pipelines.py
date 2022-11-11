from itemadapter import ItemAdapter

from .dbconfig import *
from .items import *


class AmazonusPipeline:
    def open_spider(self, spider):
        self.data = ConfigDatabase(table='data', database='amazonus')
        create_table = f"""CREATE TABLE  IF NOT EXISTS {self.data.table} (
                        `id` INT NOT NULL AUTO_INCREMENT,
                        `product_url` VARCHAR(255) NOT NULL,
                        `product_id` VARCHAR(50) NOT NULL,
                        `product_name` LONGTEXT NOT NULL,
                        `product_price` VARCHAR(45) NOT NULL,
                        `mrp` VARCHAR(45) NOT NULL,
                        `discount` VARCHAR(45) NOT NULL,
                        `shipping_charge` VARCHAR(45) NOT NULL,
                        `arrival_date` VARCHAR(255) NULL,
                        `is_sold_out` VARCHAR(45) NOT NULL,
                        `category_hierarchy` VARCHAR(1000) NOT NULL,
                        `avg_rating` VARCHAR(45) NULL,
                        `number_of_ratings` VARCHAR(45) NULL,
                        `answered_questions` VARCHAR(45) NULL,
                        `deal_of_the_day` VARCHAR(45) NULL,
                        `deal_price` VARCHAR(45) NULL,
                        `coupon` VARCHAR(255) NULL,
                        `best_seller_rank` LONGTEXT NULL,
                        `image_urls` LONGTEXT NULL,
                        `sold_by` VARCHAR(255) NULL,
                        `sold_by_link` VARCHAR(255) NULL,
                        `ships_from` VARCHAR(255) NULL,
                        `ships_from_link` VARCHAR(255) NULL,
                        `about_item` LONGTEXT NULL,
                        `product_detail` LONGTEXT NULL,
                        `product_specification` LONGTEXT NULL,
                        `scrapped_at` VARCHAR(255) NOT NULL,
                        `city` VARCHAR(255) NOT NULL,
                        PRIMARY KEY (`id`),
                        UNIQUE INDEX `product_url_UNIQUE` (`product_url` ASC) VISIBLE)
                        ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"""
        self.data.crsrSql.execute(create_table)

        self.links = ConfigDatabase(table='links', database='amazonus')
        # self.links = ConfigDatabase(table='data', database='amazonus')

    def process_item(self, item, spider):
        try:
            self.data.insertItemToSql(item)
            self.links.updateStatusSql(item)
        except Exception as e:
            print(f'{e} during insertion or update!')
        return item