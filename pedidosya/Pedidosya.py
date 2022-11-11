import pymysql, requests, json, pymongo
from unidecode import unidecode
from time import time, sleep

class ConfigDatabase():
    def __init__(self, database, table, host="localhost", user="root", password="actowiz", type1 = "sql"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = table

        self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password)
        self.connection.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.crsrSql = self.connection.cursor(pymysql.cursors.DictCursor)

        if type1 == 'mongo':
            self.connMongo = pymongo.MongoClient(f"mongodb://{host}:27017/")
            self.dbmongo = self.connMongo[self.database]
    
    def insertItemToSql(self, item):
        field_list = []
        value_list = []
        for field in item:
            field_list.append(str(field))
            value_list.append(str(item[field]).replace("'", "’"))
        fields = ','.join(field_list)
        values = "','".join(value_list)
        insert_db = f"insert into {self.table}" + "(" + fields + ") values ('" + values + "')"
        self.crsrSql.execute(insert_db)
        self.connection.commit()
    
    def insertItemToMongo(self, item):
        self.dbmongo[self.table].insert_one(item)
    
    def updateStatusSql(self, item, table=None):
        if table:
            update = f"update {table} set Status = 'Done' where URL = '{item['product_url']}'"
        else:
            # update = f"update {self.table} set Status = 'Done' where URL = '{item['product_url']}'"
            update = f"update {self.table} set Status = 'Done' where ASIN = '{item['product_id']}'"
        self.crsrSql.execute(update)
        self.connection.commit()
    
    def fetchResultsfromSql(self, fields = [], limit=None):
        fieldtofetch = ", ".join(fields) if fields else "*"
        if not limit:
            self.crsrSql.execute(f"select {fieldtofetch} from {self.table} where Status = 'Pending'")
        else:
            self.crsrSql.execute(f"select {fieldtofetch} from {self.table} where Status = 'Pending' limit {limit}")
        results = self.crsrSql.fetchall()
        return results

# -------------------------------------------------------------------------------------------------------------------------------------------------------

def Scrape():
    links = ConfigDatabase(database='pedidosya', table='links')
    
    create_table = f"""CREATE TABLE  IF NOT EXISTS {links.table} (
                        `ID` INT NOT NULL AUTO_INCREMENT,
                        `Name` VARCHAR(500) NOT NULL,
                        `City` VARCHAR(255) NOT NULL,
                        `Latitude` VARCHAR(255) NOT NULL,
                        `Longitude` VARCHAR(255) NOT NULL,
                        `URL` VARCHAR(500) NOT NULL UNIQUE,
                        `Category` VARCHAR(10) NOT NULL,
                        PRIMARY KEY (`id`))
                        ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"""
    links.crsrSql.execute(create_table)

    categories = ['Restaurant', 'Groceries', 'Drinks', 'Coffee', 'Pharmacy', 'Pets', 'Shop']

    geocodes = ConfigDatabase(database='pedidosya', table='chile_geocodes')
    geocodes.crsrSql.execute(f"select * from {geocodes.table} where Status = 'Pending'")

    for i in geocodes.crsrSql.fetchall():
        for category in categories:
            url = f"https://www.pedidosya.cl/mobile/v5/shopList?businessType={category}&country=2&max=2000&offset=0&point={i['Latitude']}%2C{i['Longitude']}"
            headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.8',
            'cookie': '_pxff_rf=1; _pxff_fp=1; __cf_bm=eyhZRrUfkFaNDYMRwu2ZlNJwtlLwGz3UkUZa9lwsEdQ-1667812967-0-AWKL+JxY93EocuX0c2aPmUXSNjjsBsRq84R5tBAz6XnWmna8+uckcGPEdrIu0fbCE8B4cjji77XF7RTyAr3jmyA=; _pxff_cc=U2FtZVNpdGU9TGF4Ow==; pxcts=be706438-5e7d-11ed-9897-744a63714b66; dhhPerseusGuestId=1667812997568.24167068525508652.0itv31alh8qp; dhhPerseusSessionId=1667812997568.711415970480871800.hq3rw828rk9; dhhPerseusHitId=1667812997568.194049713073788600.f9ppk65pf88; __Secure-peya.sid=s%3A94cbad22-39b5-4df4-be3c-86d53557daa4.u%2B7RaLcaG7v0fMUNd%2BIndrXz3HQHLXhx9uTtVNyj5Fc; __Secure-peyas.sid=s%3A8709d3e3-f8cd-4911-98cb-c1a9edd33f3a.fs%2F4FHjn6JyQOx2vrLccRLdzcY%2Bu%2B5wrN7Ri2wdboTQ; _pxvid=d0f97cb8-5e7d-11ed-8485-6b4b4f544543; _px3=da4690950a4f581b363c091e4970a8b9beb13c3ee296f3ee7c5f94d1548a8fc3:rFAXuY25sw+nJD1/SHjX94yN63hdMWmgg0p9pmiEogfP+kkV1fMQroHjlSuLbN/0mP+mcY0hOlIySI5I98UIJg==:1000:mXD1jNsbVMi0z3Fsaha35nmlwx3WMAl1X7NLPd4zhRso4/eVUuYqb7hyRjftW+XP1S+Vm+fMyOGDkImLLwwU+SzfJpnQaPfCp5WQzwaMTpzsXAdf3LLha+3uigVfzc5Dz4NuimXh2glRhYdzMZQNz6rYAWoaXZQcpVO5ygVrwbPDhyR3W82RR9vGBcOp3atKsPvF/ZpjMaxeesy/wFmucQ==; __cf_bm=VCmuIFJuGhXH0KROlFzNOSk_4c9mEQSqWB6msvDji6o-1667813303-0-ARz9b4PXbI7nKPRn13br1mhEmsRAjoemzGHd74B9ybxZYLnSH7Anf8qDUbzUBMaq2qT7OQ1saIrwHrakIBWksAY=; __Secure-peya.sid=s%3A94cbad22-39b5-4df4-be3c-86d53557daa4.u%2B7RaLcaG7v0fMUNd%2BIndrXz3HQHLXhx9uTtVNyj5Fc; __Secure-peyas.sid=s%3A8709d3e3-f8cd-4911-98cb-c1a9edd33f3a.fs%2F4FHjn6JyQOx2vrLccRLdzcY%2Bu%2B5wrN7Ri2wdboTQ; _pxhd=Dsgya385Vx5Yk7okC8XlBDcW5TuKQx/3ALSagfZmoY/Xt1ikBRAUfSNjWOIlQGEyfMiry1vuYw7sWig7StwblA==:WS/dGVdJFMpcbvUEppFKNBDnOkr-VGgnA2fz48cA4OTxTZFui2A7NVgV5-lLDO9R9fmgjt0QiIsg/mFdLSGIzz9GRCdppppvkeV/oMDdoKg=; dhhPerseusGuestId=1667812997568.24167068525508652.0itv31alh8qp; dhhPerseusHitId=1667813328271.906822312923506400.xo9jbr593; dhhPerseusSessionId=1667812997568.711415970480871800.hq3rw828rk9',
            'referer': 'https://www.pedidosya.cl/restaurantes?address=Street%20Wrap%20Providencia&city=Santiago&lat=-33.4285439&lng=-70.6030084',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
            }

            response = requests.request("GET", url, headers=headers)
            
            if response.status_code == 200:
                jsonData = json.loads(response.text)
                totalcount = jsonData['swimlanes']['info']['totalRestaurants']
                
                if totalcount < 2000:
                    for count, j in enumerate(jsonData['list']['data'], start=1):
                        item = {}
                        city = unidecode(j['cityName'].casefold())
                        if ' ' in city:
                            city = city.replace(' ', '-')
                        item['URL'] = f"https://www.pedidosya.cl/restaurantes/{city}/{j['link']}-menu"
                        item['Name'] = j['name']
                        item['City'] = j['cityName']
                        item['Latitude'] = j['latitude']
                        item['Longitude'] = j['longitude']
                        item['Category'] = category
                        
                        try:
                            links.insertItemToSql(item)
                            print(f'\n{count}. {item}\n')
                        except Exception as e:
                            print(f'{e} during injection!')

                else:
                    print(f'Total restaurants are {totalcount}.')
                    print(response.url)
                    exit()
                
                if totalcount > 0:
                    geocodes.crsrSql.execute(f"update {geocodes.table} set Status = 'Done' where Latitude = '{i['Latitude']}' and Longitude = '{i['Longitude']}'")
                    geocodes.connection.commit()
                else:
                    geocodes.crsrSql.execute(f"update {geocodes.table} set Status = 'None' where Status = 'Pending' and Latitude = '{i['Latitude']}' and Longitude = '{i['Longitude']}'")
                    geocodes.connection.commit()
                
                sleep(5)
            
            else:
                
                print(f'\nResponse Error')
                exit()
    
    
if __name__ == '__main__':
    Scrape()