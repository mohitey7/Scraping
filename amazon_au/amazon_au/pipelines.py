from itemadapter import ItemAdapter
from .items import AmazonAuData, AmazonAuLinks
from .dbconfig import *

class AmazonAuPipeline:
    def open_spider(self, spider):
        self.links = ConfigSql(table='links', database='amazonau')
        try:
            query = f"""CREATE TABLE IF NOT EXISTS {self.links.table}(`ID` int(11) NOT NULL AUTO_INCREMENT,
                        `URL` varchar(255) NOT NULL UNIQUE,
                        `Status` varchar(10) DEFAULT 'Pending',
                        PRIMARY KEY (`ID`))"""
            self.links.cursor.execute(query)
        except Exception as e:
            print(e)
        
        self.data = ConfigSql(table='data', database='amazonau')
        try:
            query = f"""CREATE TABLE IF NOT EXISTS {self.data.table}(`ID` int(11) NOT NULL AUTO_INCREMENT,
                        `Product` varchar(255) NOT NULL,
                        `Brand` varchar(50) NOT NULL,
                        `Price` varchar(50) NOT NULL,
                        `Ratings` varchar(10) NOT NULL,
                        `Reviews` varchar(10) NOT NULL,
                        `Availability` varchar(20) NOT NULL,
                        `URL` varchar(255) NOT NULL UNIQUE,
                        `Scraped_at` varchar(255) NOT NULL,
                        PRIMARY KEY (`ID`))"""
            self.data.cursor.execute(query)
        except Exception as e:
            print(f'{e} during table creation!')

    def process_item(self, item, spider):
        if isinstance(item, AmazonAuLinks):
            try:
                self.links.insert(item)
                return item
            except Exception as e:
                print(f'{e} during insertion!')

        if isinstance(item, AmazonAuData):
            try:
                self.data.insert(item)
                self.data.update(item)
                return item
            except Exception as e:
                print(f'{e} during SQL injection!')