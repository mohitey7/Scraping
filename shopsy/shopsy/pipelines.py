from itemadapter import ItemAdapter
from .items import *
from .dbconfig import *

class ShopsyPipeline:
    def open_spider(self, spider):
        self.data = ConfigSql(table='data', database='shopsy')
        try:
            query = f"""CREATE TABLE IF NOT EXISTS {self.data.table}(`ID` int(11) NOT NULL AUTO_INCREMENT,
                        `ProductID` varchar(50) NOT NULL UNIQUE,
                        `CategoryL1` varchar(255) NOT NULL,
                        `CategoryL2` varchar(255) NOT NULL,
                        `URL` varchar(500) NOT NULL UNIQUE,
                        PRIMARY KEY (`ID`))"""
            self.data.cursor.execute(query)
        except Exception as e:
            print(f'{e} during data table creation!')
        
        self.links = ConfigSql(table='links', database='shopsy')

    def process_item(self, item, spider):
        try:
            self.data.insert(item)
            self.links.update(item)
            return item
        except Exception as e:
            print(f'{e} during insertion!')