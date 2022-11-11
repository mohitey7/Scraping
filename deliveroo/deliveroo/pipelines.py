# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymysql

class DeliverooPipeline:
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='deliveroo.ie')
    cursor = connection.cursor()
    
    def __init__(self):
        DeliverooPipeline.cursor.execute("""CREATE TABLE if not exists `dublin restaurants data` (
                                            `ID` INT NOT NULL AUTO_INCREMENT,
                                            `Restaurant_Name` VARCHAR(255) NOT NULL,
                                            `Cuisines` VARCHAR(255) NOT NULL,
                                            `Rating` VARCHAR(20) NOT NULL,
                                            `Reviews` VARCHAR(10) NOT NULL,
                                            `Description` VARCHAR(1000) NOT NULL,
                                            `Delivery_Fee` VARCHAR(20) NOT NULL,
                                            `Minimum_Order_Amount` VARCHAR(10) NOT NULL,
                                            `Contact_Number` VARCHAR(20) NOT NULL,
                                            `Address` VARCHAR(255) NOT NULL,
                                            `URL` VARCHAR(255) NOT NULL,
                                            PRIMARY KEY (`ID`),
                                            UNIQUE INDEX `URL_UNIQUE` (`URL` ASC) VISIBLE);""")

    def process_item(self, item, spider):
        test = self.cursor.execute(f"""insert into `dublin restaurants data` (Restaurant_Name, Cuisines, Rating, Reviews, Description, Delivery_Fee, Minimum_Order_Amount, Contact_Number, Address, URL) values ("{item['name']}", "{item['cuisines']}", "{item['rating']}", "{item['reviews']}", "{item['description']}", "{item['delivery_fee']}", "{item['minimum_order_amount']}", "{item['contact_number']}", "{item['address']}", "{item['url']}")""")
        self.cursor.execute(f"UPDATE `dublin restaurants` SET Status = 'Completed' WHERE Restaurants = '{item['url']}'")
        self.connection.commit()
        return item