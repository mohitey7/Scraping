from itemadapter import ItemAdapter
from .items import PedidosyaLinks
from .dbconfig import ConfigSql

class PedidosyaPipeline:
    def open_spider(self, spider):
        self.links = ConfigSql(table='links', database='pedidosya')
        try:
            query = f"""CREATE TABLE  IF NOT EXISTS {self.links.table} (
                        `ID` INT NOT NULL AUTO_INCREMENT,
                        `Name` VARCHAR(500) NOT NULL,
                        `City` VARCHAR(255) NOT NULL,
                        `Latitude` VARCHAR(255) NOT NULL,
                        `Longitude` VARCHAR(255) NOT NULL,
                        `URL` VARCHAR(500) NOT NULL UNIQUE,
                        `Category` VARCHAR(10) NOT NULL,
                        PRIMARY KEY (`id`))
                        ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"""
            self.links.cursor.execute(query)
        except Exception as e:
            print(f'{e} during links table creation')

    def process_item(self, item, spider):
        if isinstance(item, PedidosyaLinks):
            try:
                self.links.insert(item)
                return item
            except Exception as e:
                print(f'{e} during insertion!')
