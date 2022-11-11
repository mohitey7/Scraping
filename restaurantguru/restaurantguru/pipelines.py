from itemadapter import ItemAdapter
from .items import *
import pymysql

class RestaurantguruPipeline:
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='restaurantguru')
    cursor = connection.cursor()

    def __init__(self):

        RestaurantguruPipeline.cursor.execute("""CREATE TABLE if not exists `links2` (
                                            `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
                                            `RestroURL` VARCHAR(255) NOT NULL,
                                            `PageURL` VARCHAR(255) NOT NULL,
                                            `CategoryURL` VARCHAR(255) NOT NULL,
                                            `HashID` VARCHAR(255) NOT NULL UNIQUE,
                                            `Status` VARCHAR(10) NOT NULL DEFAULT 'Pending');""")
        
        # RestaurantguruPipeline.cursor.execute("""CREATE TABLE if not exists `links` (
        #                                     `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
        #                                     `URL` VARCHAR(255) NOT NULL UNIQUE,
        #                                     `Status` VARCHAR(10) NOT NULL DEFAULT 'Pending');""")

        RestaurantguruPipeline.cursor.execute("""CREATE TABLE if not exists `data` (
                                        `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
                                        `Restaurant` VARCHAR(50) NOT NULL,
                                        `Latitude` VARCHAR(20) NOT NULL,
                                        `Longitude` VARCHAR(20) NOT NULL,
                                        `URL` VARCHAR(255) NOT NULL UNIQUE);""")

    def process_item(self, item, spider):
        try:
            if isinstance(item, RestaurantguruLinks):
                self.cursor.execute(f"""insert into `links2` (RestroURL, PageURL, CategoryURL, HashID) values ("{item['restroUrl']}", "{item['pageUrl']}", "{item['categoryUrl']}", "{item['hashID']}")""")
                # self.cursor.execute(f"""insert into `links` (URL) values ("{item['url']}")""")

            if isinstance(item, RestaurantguruData):
                try:
                    self.cursor.execute(f"""insert into `data` (Restaurant, Latitude, Longitude, URL) values ("{item['restaurant']}", "{item['latitude']}", "{item['longitude']}", "{item['url']}")""")
                except:
                    self.cursor.execute(f"""insert into `data` (Restaurant, Latitude, Longitude, URL) values ('{item['restaurant']}', "{item['latitude']}", "{item['longitude']}", "{item['url']}")""")
                    # self.cursor.execute("insert into `data` (Restaurant, Latitude, Longitude, URL) values ("""+{item['restaurant']}+""", """+{item['latitude']}+""", """+{item['longitude']}+""", """+{item['url']}+""")")
                    
                self.cursor.execute(f"UPDATE `links` SET `Status` = 'Completed' WHERE `URL` = '{item['url']}'")
            self.connection.commit()
        except Exception as e:
            print(e)
        return item