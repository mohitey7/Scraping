from itemadapter import ItemAdapter
from .items import *
from .dbconfig import *

class JahezPipeline:
    def open_spider(self, spider):
        self.links = ConfigDatabase(table='links', database='jahez')
        try:
            query = f"""CREATE TABLE IF NOT EXISTS `{self.links.table}`
                                (`ID` INT NOT NULL AUTO_INCREMENT,
                                `Restaurant` varchar(255) NOT NULL,
                                `URL` varchar(255) NOT NULL UNIQUE,
                                `Status` varchar(15) NOT NULL DEFAULT 'Pending',
                                PRIMARY KEY (`ID`)
                                ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"""
            self.links.crsrSql.execute(query)

        except Exception as e:
            print(f'{e} during {self.links.table} creation!')
        
        self.data = ConfigDatabase(table='data', database='jahez')
        try:
            query = f"""CREATE TABLE IF NOT EXISTS `{self.data.table}`
                                (`ID` INT NOT NULL AUTO_INCREMENT,
                                `Restaurant` varchar(255) NOT NULL,
                                `Category` varchar(255) NOT NULL,
                                `Item` varchar(255) NOT NULL,
                                `Price` varchar(10) NOT NULL,
                                `Description` LONGTEXT NOT NULL,
                                `ImageURL` varchar(500) NOT NULL,
                                `URL` varchar(500) NOT NULL,
                                `Hash` varchar(255) NOT NULL UNIQUE,
                                PRIMARY KEY (`ID`)
                                ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"""
            self.data.crsrSql.execute(query)

        except Exception as e:
            print(f'{e} during {self.data.table} creation!')

        # self.data = ConfigDatabase(table='data', database='jahez', type1='mongo')
       
    def process_item(self, item, spider):
        if isinstance(item, JahezLinks):
            try:
                self.links.insertItemToSql(item)
            except Exception as e:
                print(e)
                spider.logger.error(e)
            return item
        
        if isinstance(item, JahezData):
            try:
                self.data.insertItemToSql(item)
                # self.data.insertItemToMongo(item)
            except Exception as e:
                print(e)
                spider.logger.error(e)
            return item

    # def close_spider(self, spider):
    #     links = ConfigDatabase(table='links', database='jahez')
    #     query = f"update links set Status = 'Pending' where Status = 'Completed'"
    #     links.crsrSql.execute(query)