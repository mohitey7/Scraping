from itemadapter import ItemAdapter
from .items import *
from .dbconfig import *


class MamaearthPipeline:
    def open_spider(self, spider):
        self.links = ConfigSql(table='links', database='mamaearth')
        try:
            query = f"""CREATE TABLE IF NOT EXISTS {self.links.table}(`ID` int(11) NOT NULL AUTO_INCREMENT,
                        `URL` varchar(255) NOT NULL UNIQUE,
                        `Status` varchar(10) DEFAULT 'Pending',
                        PRIMARY KEY (`ID`))"""
            self.links.cursor.execute(query)
        except Exception as e:
            print(f'{e} during links table creation')
        
        self.data = ConfigSql(table='data', database='mamaearth')
        try:
            query = f"""CREATE TABLE IF NOT EXISTS {self.data.table}(`ID` int(11) NOT NULL AUTO_INCREMENT,
                        `Product` varchar(1000) NOT NULL,
                        `Subtitle` varchar(1000) NULL,
                        `Category` varchar(255) NOT NULL,
                        `SKU` varchar(50) NOT NULL,
                        `Price` varchar(10) NULL,
                        `MRP` varchar(10) NULL,
                        `SoldOut` varchar(10) NOT NULL,
                        `Rating` varchar(50) NULL,
                        `ReviewCount` varchar(10) NULL,
                        `AverageRatingPercent` varchar(10) NULL,
                        `Images` LONGTEXT NULL,
                        `Quantity` varchar(20) NOT NULL,
                        `CreatedAt` varchar(255) NOT NULL,
                        `UpdatedAt` varchar(255) NOT NULL,
                        `URL` varchar(255) NOT NULL UNIQUE,
                        `Scraped_at` varchar(255) NOT NULL,
                        PRIMARY KEY (`ID`))"""
            self.data.cursor.execute(query)
        except Exception as e:
            print(f'{e} during data table creation!')

    def process_item(self, item, spider):
        if isinstance(item, MamaearthLinks):
            try:
                self.links.insert(item)
                return item
            except Exception as e:
                print(f'{e} during insertion!')

        if isinstance(item, MamaearthData):
            try:
                self.data.insert(item)
                self.links.update(item)
                return item
            except Exception as e:
                print(f'{e} during SQL injection!')