from itemadapter import ItemAdapter
from .items import *
import pymysql

class FlipkartPipeline:
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='flipkart')
    cursor = connection.cursor()

    def __init__(self):
        FlipkartPipeline.cursor.execute("""CREATE TABLE if not exists `links` (
                                            `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
                                            `URL` VARCHAR(255) NOT NULL UNIQUE,
                                            `Name` VARCHAR(255) NOT NULL,
                                            `Latitude` VARCHAR(255) NOT NULL,
                                            `Longitude` VARCHAR(255) NOT NULL,
                                            `ID` VARCHAR(255) NOT NULL UNIQUE,
                                            `Status` VARCHAR(10) NOT NULL DEFAULT 'Pending');""")

        FlipkartPipeline.cursor.execute("""CREATE TABLE if not exists `data` (
                                            `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
                                            `Product` VARCHAR(255) NOT NULL UNIQUE,
                                            `Price` VARCHAR(10) NOT NULL,
                                            `Average_Rating` VARCHAR(10) NOT NULL,
                                            `Ratings` VARCHAR(10) NOT NULL,
                                            `Reviews` VARCHAR(10) NOT NULL,
                                            `Discount` VARCHAR(5) NOT NULL,
                                            `Availablity` VARCHAR(5) NOT NULL,
                                            `Category` VARCHAR(255) NOT NULL,
                                            `Images` LONGTEXT NOT NULL,
                                            `URL` VARCHAR(255) NOT NULL UNIQUE,
                                            `ScrappedTime` VARCHAR(30) NOT NULL);""")

    def process_item(self, item, spider):
        try:
            if isinstance(item, FlipkartLinks):
                self.cursor.execute(f"""insert into `links` (URL) values ("{item['url']}")""")
            if isinstance(item, FlipkartData):
                self.cursor.execute(f"""insert into `data` (Product, Price, Average_Rating, Ratings, Reviews, Discount, Availablity, Category, Images, URL, ScrappedTime) values ("{item['productTitle']}", "{item['price']}", "{item['averageRating']}", "{item['ratings']}", "{item['reviews']}", "{item['discount']}", "{item['availablity']}", "{item['category']}", "{item['images']}", "{item['url']}", "{item['scrappedTime']}")""")
                self.cursor.execute(f"UPDATE `links` SET `Status` = 'Completed' WHERE `URL` = '{item['url']}'")
            self.connection.commit()
        except Exception as e:
            print(e)
        return item