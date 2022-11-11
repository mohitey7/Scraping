from itemadapter import ItemAdapter
from .items import *
import pymysql

class MenulogPipeline:
    connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='menulog')
    cursor = connection.cursor()

    def __init__(self):
        MenulogPipeline.cursor.execute("""CREATE TABLE if not exists `links3` (
                                            `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
                                            `URL` VARCHAR(255) NOT NULL UNIQUE,
                                            `Status` VARCHAR(10) NOT NULL DEFAULT 'Pending');""")

        MenulogPipeline.cursor.execute("""CREATE TABLE if not exists `data2` (
                                        `ID` INT NOT NULL AUTO_INCREMENT UNIQUE,
                                        `URL` VARCHAR(255) NOT NULL UNIQUE,
                                        `Location` VARCHAR(20) NOT NULL,
                                        `City` VARCHAR(20) NOT NULL,
                                        `Name` VARCHAR(255) NOT NULL,
                                        `Cuisines` VARCHAR(255) NOT NULL,
                                        `Ratings` VARCHAR(10) NOT NULL,
                                        `Reviews` VARCHAR(10) NOT NULL,
                                        `StreetAddress` VARCHAR(255) NOT NULL,
                                        `AddressLocality` VARCHAR(20) NOT NULL,
                                        `AddressRegion` VARCHAR(5),
                                        `PostalCode` VARCHAR(10) NOT NULL,
                                        `DeliveryTime` VARCHAR(10) NOT NULL,
                                        `RestaurantID` VARCHAR(10) NOT NULL UNIQUE,
                                        `Phone` VARCHAR(20) NOT NULL,
                                        `Note` LONGTEXT NOT NULL,
                                        `MinOrder` VARCHAR(20) NOT NULL,
                                        `DeliveryFee` VARCHAR(20) NOT NULL,
                                        `FullAddress` VARCHAR(255) NOT NULL,
                                        `AboutUs` LONGTEXT NOT NULL,
                                        `DeliveryHours` LONGTEXT NOT NULL,
                                        `Offer` VARCHAR(100) NOT NULL,
                                        `Latitude` VARCHAR(20) NOT NULL,
                                        `Longitude` VARCHAR(20) NOT NULL,
                                        `StampCard` VARCHAR(20) NOT NULL,
                                        `HashID` VARCHAR(20) NOT NULL UNIQUE);""")

    def process_item(self, item, spider):
        try:
            if isinstance(item, MenulogLinks):
                self.cursor.execute(f"""insert into `links3` (URL) values ("{item['url']}")""")
            if isinstance(item, MenulogData):
                self.cursor.execute(f"""insert into `data2` (URL, Location, City, Name, Cuisines, Ratings, Reviews, StreetAddress, AddressLocality, PostalCode, DeliveryTime, RestaurantID, Phone, Note, MinOrder, DeliveryFee, FullAddress, AboutUs, DeliveryHours, Offer, Latitude, Longitude, StampCard, HashID) values ("{item['url']}", "{item['city']}", "{item['city']}", "{item['name']}", "{item['cuisines']}", "{item['rating']}", "{item['review']}", "{item['streetAddress']}", "{item['addressLocality']}", "{item['postcode']}", "{item['deliveryTime']}", "{item['restaurantID']}", "{item['phone']}", "{item['note']}", "{item['minimumAmount']}", "{item['DeliveryFee']}", "{item['FullAddress']}", "{item['about']}", "{item['deliveryhours']}", "{item['offer']}", "{item['latitude']}", "{item['longitude']}", "{item['StampCard']}", "{item['hashid']}")""")
                self.cursor.execute(f"UPDATE `links3` SET `Status` = 'Completed' WHERE `URL` = '{item['url']}'")
            self.connection.commit()
        except Exception as e:
            print(e)
        return item