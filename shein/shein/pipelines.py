from itemadapter import ItemAdapter
from .items import *
from .databaseConfig import *

class SheinPipeline:
    def open_spider(self, spider):
        try:
            self.ob = ConfigDatabase(table='data2', database='shein')
            
        except Exception as e:
            spider.logger.info(e)

        try:
            create_table = f"""CREATE TABLE IF NOT EXISTS `{self.ob.table}` (`Id` INT NOT NULL AUTO_INCREMENT,
                                `input_pid` VARCHAR (40) default null,
                                `product_id` VARCHAR (40) NOT null,
                                `catalog_name` VARCHAR (255) NOT null,
                                `catalog_id` VARCHAR (40) null,
                                `source` VARCHAR (40) DEFAULT 'shein',
                                `scraped_date` VARCHAR (40) NOT null,
                                `product_name` LONGTEXT NOT null,
                                `image_url` VARCHAR (1000) DEFAULT 'N/A',
                                `category_hierarchy` VARCHAR (500) NOT null,
                                `product_price` VARCHAR (255) DEFAULT 'N/A',
                                `arrival_date` VARCHAR (40) DEFAULT 'N/A',
                                `shipping_charges` VARCHAR (40) DEFAULT 'N/A',
                                `is_sold_out` VARCHAR (40) DEFAULT 'false',
                                `discount` VARCHAR (40) DEFAULT 'N/A',
                                `mrp` VARCHAR (40) DEFAULT 'N/A',
                                `page_url` VARCHAR (1000) DEFAULT 'N/A',
                                `product_url` VARCHAR (1000) NOT null,
                                `number_of_ratings` VARCHAR (40) DEFAULT 'N/A',
                                `avg_rating` VARCHAR (40) DEFAULT 'N/A',
                                `position` VARCHAR (4) default null,
                                `country_code` VARCHAR (2) DEFAULT 'SG',
                                `others` LONGTEXT,
                                `status` varchar(40) default null,
                                `l1` varchar(100) default null,
                                `l2` varchar(100) default null,
                                `l3` varchar(100) default null,
                                `l4` varchar(100) default null,
                                PRIMARY KEY (`Id`),
                                UNIQUE KEY `product_zip` (`product_id`)
                                ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"""
            
            self.ob.crsrSql.execute(create_table)

        except Exception as e:
            print(e)

    def process_item(self, item, spider):
        if isinstance(item, SheinData):
            try:
                self.ob.insertItemToSql(item)
                # self.ob.updateStatusSql(item)
            except Exception as e:
                print(e)
                spider.logger.error(e)
            return item