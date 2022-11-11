# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from itemadapter import ItemAdapter
import pymysql
from .items import HarrisfarmsData, HarrisfarmsUrls

class HarrisfarmsPipeline:
   
    def __init__(self):
        self.connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='harrisfarms')
        self.cursor = self.connection.cursor()
        self.cursor.execute("""CREATE TABLE if not exists `links` (
                            `ID` INT NOT NULL AUTO_INCREMENT,
                            `Links` VARCHAR(255) NOT NULL,
                            `Status` VARCHAR(15) NOT NULL DEFAULT 'Pending',
                            UNIQUE INDEX `ID_UNIQUE` (`ID` ASC) VISIBLE,
                            UNIQUE INDEX `Links_UNIQUE` (`Links` ASC) VISIBLE);""")
        self.cursor.execute("""CREATE TABLE if not exists `data` (
                                `ID` INT NOT NULL AUTO_INCREMENT,
                                `Title` VARCHAR(255) NOT NULL,
                                `Description` LONGTEXT NOT NULL,
                                `Images` VARCHAR(1000) NOT NULL,
                                `URL` VARCHAR(255) NOT NULL,
                                UNIQUE INDEX `ID_UNIQUE` (`ID` ASC) VISIBLE,
                                UNIQUE INDEX `Title_UNIQUE` (`Title` ASC) VISIBLE,
                                UNIQUE INDEX `URL_UNIQUE` (`URL` ASC) VISIBLE);""")

    def process_item(self, item, spider):
        if isinstance(item, HarrisfarmsUrls):
            try:
                self.cursor.execute(f"""insert into links (Links) values ("{item['link']}")""")
            except:
                pass
        if isinstance(item, HarrisfarmsData):
            self.cursor.execute(f"""insert into data (Title, Description, Images, URL) values ("{item['title']}", "{item['description']}", '{item['images']}', '{item['link']}')""")
            self.cursor.execute(f"UPDATE links SET status = 'Completed' WHERE Links = '{item['link']}'")
            self.connection.commit()
            return item