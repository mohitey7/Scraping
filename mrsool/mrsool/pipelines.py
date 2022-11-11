from itemadapter import ItemAdapter
import pymysql
from .items import *

class MrsoolPipeline:
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='mrsool')
    cursor = connection.cursor()
    
    def __init__(self):
        MrsoolPipeline.cursor.execute("""CREATE TABLE if not exists `links` (
                                        `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
                                        `URL` VARCHAR(255) NOT NULL UNIQUE,
                                        `Name` VARCHAR(255) NOT NULL,
                                        `Latitude` VARCHAR(255) NOT NULL,
                                        `Longitude` VARCHAR(255) NOT NULL,
                                        `ShopID` VARCHAR(255) NOT NULL UNIQUE,
                                        `Status` VARCHAR(10) NOT NULL DEFAULT 'Pending');""")
        
        MrsoolPipeline.cursor.execute("""CREATE TABLE if not exists `data` (
                                        `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
                                        `Restaurant` VARCHAR(255) NOT NULL,
                                        `Category` VARCHAR(255) NOT NULL,
                                        `MenuItem` VARCHAR(255) NOT NULL,
                                        `Description` LONGTEXT NOT NULL,
                                        `Price` VARCHAR(10) NOT NULL,
                                        `URL` VARCHAR(255) NOT NULL,
                                        `HashID` VARCHAR(50) NOT NULL UNIQUE);""")

    def process_item(self, item, spider):
        try:
            if isinstance(item, MrsoolLinks):
                try:
                    self.cursor.execute(f"""insert into `links` (URL, Name, Latitude, Longitude, ShopID) values ("{item['url']}", "{item['name']}", "{item['latitude']}", "{item['longitude']}", "{item['shopID']}")""")
                except:
                    self.cursor.execute(f"""insert into `links` (URL, Name, Latitude, Longitude, ShopID) values ("{item['url']}", '{item['name']}', "{item['latitude']}", "{item['longitude']}", "{item['shopID']}")""")
            if isinstance(item, MrsoolData):
                try:
                    self.cursor.execute(f"""insert into `data` (Restaurant, Category, MenuItem, Description, Price, URL, HashID) values ("{item['restaurant']}", "{item['category']}", "{item['menuitem']}", "{item['longdesc']}", "{item['price']}", "{item['url']}", "{item['hashID']}")""")
                except:
                    self.cursor.execute(f"""insert into `data` (Restaurant, Category, MenuItem, Description, Price, URL, HashID) values ("{item['restaurant']}", '{item['category']}', '{item['menuitem']}', '{item['longdesc']}', "{item['price']}", "{item['url']}", "{item['hashID']}")""")
            #     self.cursor.execute(f"UPDATE `links` SET `Status` = 'Completed' WHERE `URL` = '{item['url']}'")
            self.connection.commit()
        except Exception as e:
            print(e)
        return item
