# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymysql
from .items import *

class YelpPipeline:
    connection = pymysql.connect(host='localhost', user='root', password='workbench', database='yelp')
    cursor = connection.cursor()
    
    def __init__(self):
        self.cursor.execute("""CREATE TABLE if not exists `Company URLs` (
                                `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
                                `URL` VARCHAR(255) NOT NULL UNIQUE,
                                `Status` VARCHAR(15) NOT NULL DEFAULT 'Pending');""")

        self.cursor.execute("""CREATE TABLE if not exists `Company Data` (
                                `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
                                `Company` VARCHAR(255) NOT NULL,
                                `Owner` VARCHAR(20) NOT NULL,
                                `Contact` VARCHAR(20) NOT NULL,
                                `Address` VARCHAR(255) NOT NULL,
                                `Website` VARCHAR(255) NOT NULL UNIQUE,
                                `Page_URL` VARCHAR(255) NOT NULL UNIQUE);""")
        

    def process_item(self, item, spider):
        if isinstance(item, YelpItem):
            try:
                self.cursor.execute(f"""insert into `Company URLs` (URL) values ("{item['company']}")""")
            except:
                pass
        if isinstance(item, YelpData):
            self.cursor.execute(f"""insert into `company data` (Company, Owner, Contact, Address, Website, Page_URL) values ('{item['company']}', "{item['business_owner']}", '{item['contact_number']}', "{item['address']}", '{item['website']}', '{item['page_url']}')""")
            self.cursor.execute(f"UPDATE `company urls` SET Status = 'Completed' WHERE URL = '{item['page_url']}'")
        self.connection.commit()
        return item