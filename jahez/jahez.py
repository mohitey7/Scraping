import requests, json, pymysql, pymongo, hashlib, os, pandas, time
from datetime import date

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
    
    def updateStatusSql(self, item):
        try:
            update = f"update {self.table} set Status = 'Done' where URL = '{item['URL']}'"
            self.crsrSql.execute(update)
            self.connection.commit()
        except Exception as e:
            print(f'{e} during status update!')
    
    def fetchResultsfromSql(self, fields = [], limit=None, table=None):
        fieldtofetch = ", ".join(fields) if fields else "*"
        if limit and not table:
            self.crsrSql.execute(f"select {fieldtofetch} from {self.table} where Status = 'Pending' limit {limit}")
        elif not limit and not table:
            self.crsrSql.execute(f"select {fieldtofetch} from {self.table} where Status = 'Pending'")
        elif table and not limit:
            self.crsrSql.execute(f"select {fieldtofetch} from {table} where Status = 'Pending'")
        elif table and limit:
            self.crsrSql.execute(f"select {fieldtofetch} from {table} where Status = 'Pending' limit {limit}")
        results = self.crsrSql.fetchall()
        return results

# -----------------------------------------------------------------------------------------------------------------------------------------

# Running starts here!

class Scrapping(ConfigDatabase):
    def __init__(self):
        logpath = f'{os.getcwd()}\\Log\\{date.today().strftime("%Y%m%d")}\\'
        if not os.path.exists(logpath):
            os.makedirs(logpath)
        
        filepath = f'{os.getcwd()}\\Files\\{date.today().strftime("%Y%m%d")}\\'
        if not os.path.exists(filepath):
            os.makedirs(filepath)

        self.links = ConfigDatabase(table='links', database='jahez')
        try:
            create_table_query = f"""CREATE TABLE IF NOT EXISTS `{self.links.table}`
                                (`ID` INT NOT NULL AUTO_INCREMENT,
                                `Restaurant` varchar(255) NOT NULL,
                                `URL` varchar(255) NOT NULL UNIQUE,
                                `Status` varchar(15) NOT NULL Default 'Pending',
                                PRIMARY KEY (`ID`)
                                ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"""
            self.links.crsrSql.execute(create_table_query)

        except Exception as e:
            with open(f'{os.getcwd()}\Log\{date.today().strftime("%Y%m%d")}\{date.today().strftime("%Y%m%d")}.txt', 'a', encoding='utf_8') as f:
                f.write(f'{e} during {self.links.table} creation!\n\n')
                input("{self.links.table} failed to created due to an error! Program will now exit.")

        # --------------------------------------------------------------------
        
        self.data = ConfigDatabase(table=f'data_{date.today().strftime("%Y%m%d")}', database='jahez')
        try:
            query = f"""CREATE TABLE IF NOT EXISTS `{self.data.table}`
                                (`ID` INT NOT NULL AUTO_INCREMENT,
                                `Restaurant` varchar(255) NOT NULL,
                                `Category` varchar(255) NOT NULL,
                                `Item` varchar(255) NOT NULL,
                                `Price` varchar(10) NOT NULL,
                                `Description` LONGTEXT NOT NULL,
                                `ImageURL` varchar(500) NOT NULL,
                                `URL` varchar(500) NOT NULL,
                                `Hash` varchar(255) NOT NULL UNIQUE,
                                PRIMARY KEY (`ID`)
                                ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"""
            self.data.crsrSql.execute(query)
        
        except Exception as e:
            with open(f'{os.getcwd()}\Log\{date.today().strftime("%Y%m%d")}\{date.today().strftime("%Y%m%d")}.txt', 'a', encoding='utf_8') as f:
                f.write(f'{e} during {self.data.table} creation!\n\n')
                input("{self.links.table} failed to created due to an error! Program will now exit.")

    def linksExtraction(self):
        geocodes = ['21.4925&39.17757', '18.329384&42.759365', '25.994478&45.318161', '26.094088&43.973454', '21.437273&40.512714', '29.953894&40.197044', '30.983334&41.016666', '24.186848&38.026428', '27.523647&41.696632', '24.774265&46.738586', '28.446959&45.948944', '26.39925&49.98436', '16.909683&42.567902', '24.470901&39.612236', '24.507143&44.408798', '21.42251&39.826168', '26.565191&49.996376', '26.236355&50.0326', '24.68773&46.72185', '21.49012&39.18624', '21.42664&39.82563', '24.46861&39.61417', '26.43442&50.10326', '24.49258&39.58572', '28.3998&36.57151', '26.32599&43.97497', '25.40768&49.59028', '21.27028&40.41583', '17.49326&44.12766', '24.15541&47.33457', '18.21639&42.50528', '24.08954&38.0618', '18.3&42.73333', '25.36467&49.58764', '28.43279&45.97077', '27.52188&41.69073', '25.86944&43.4973', '27.0174&49.62251', '24.6500&46.7100', '21.5428&39.1728', '21.4225&39.8261', '24.4667&39.6000', '26.4333&50.1000', '25.3608&49.5997', '26.3333&43.9667', '23.4895&46.7564', '21.2667&40.4167', '28.3838&36.5550', '18.3000&42.7333', '27.5236&41.7001', '26.5196&50.0115', '25.4100&49.5808', '24.1556&47.3120', '17.4917&44.1322', '24.0943&38.0493', '18.2167&42.5000', '30.9833&41.0167', '16.8892&42.5611', '30.0000&40.1333', '20.0129&41.4677', '24.65&46.71', '21.5428&39.1728', '21.4225&39.8261', '24.4667&39.6', '26.4333&50.1', '25.3608&49.5997', '23.4895&46.7564', '21.2667&40.4167', '28.3838&36.555', '26.3333&43.9667', '27.5236&41.7001', '26.5196&50.0115', '25.4416&49.6642', '24.1556&47.312', '24.0943&38.0493', '28.4337&45.9601', '27.0046&49.646', '18.2167&42.5', '26.3&50.2167', '30&40.1333', '16.8892&42.5611', '17.4917&44.1322', '30.9833&41.0167', '20.0129&41.4677', '18.3&42.7333', '20.0087&42.5987', '29.6202&43.4948', '26.2324&36.4636', '26.2667&50.15', '20.4623&45.5722', '29.8153&39.8664', '24.7333&46.5756', '21.4411&40.4975', '25.9039&45.3456', '24.6173&46.2256', '22.2833&46.7333', '21.2142&41.6331', '31.3333&37.3333', '24.0737&45.2806', '19.1264&41.0789', '24.65&46.77', '21.5&39.17', '21.43&39.82', '24.48&39.59', '26.43&50.1', '21.26&40.38', '28.39&36.57', '26.37&43.97', '25.35&49.58', '25.43&49.57', '18.31&42.73', '27.53&41.7', '24.18&47.5', '26.23&50.2', '28.43&45.96', '26.27&50.2', '18.23&42.5', '30.99&41.02', '26.52&50.02', '28.98&38.57', '26.09&43.99', '17.5&44.13', '24.09&38.05', '27.01&49.65', '26.29&50.16', '31.33&37.34', '29.97&40.2', '26.47&50.05', '25.87&43.5', '16.9&42.55', '28.42&48.51', '26.7&50.06', '20.01&42.6', '26.3&44.8', '29.81&39.87', '26.57&50.07', '24.49&44.38', '26.65&49.96', '17.47&47.12', '16.97&42.83', '17.15&42.62', '31.67&38.66', '22.8&39.02', '25.93&49.67', '29.63&43.5', '24&47.17', '23.92&42.93', '25.02&37.27', '24.74&42.92', '20.02&41.47', '24.713552&46.675296', '21.285407&39.237551', '21.389082&39.857912', '24.524654&39.569184', '24.5&39.58', '26.392667&49.977714', '21.437273&40.512714', '28.383508&36.566191', '26.359231&43.981812', '18.309339&42.766233', '25.314156&49.629908', '25.405105&49.548367', '25.405105&49.548367', '17.565604&44.228944', '26.959771&49.568742', '18.246468&42.511724', '24.023176&38.189978', '26.217191&50.197138', '26.083398&43.962749', '30.959945&41.059564', '29.878003&40.104306', '16.889359&42.570567', '31.329635&37.361353', '26.236125&50.039302', '26.576492&49.998236', '20.272274&41.441251', '20.272274&41.441251', '19.976352&42.590167', '25.851747&43.522231', '21.074424&40.324176', '26.491459&50.0085', '25.856898&44.224211', '28.425662&48.488722', '24.5167&44.418179', '17.154798&42.626897', '26.309854&44.831834', '16.969248&42.844291', '26.627478&49.916189', '22.79067&39.018962', '24.773771&46.713538', '31.666078&38.663469', '23.90522&42.912495', '30.515478&38.221649', '23.976619&47.155709', '25.026409&37.27076', '26.550264&37.967865', '25.916815&49.670721', '23.755863&38.757761', '16.597044&42.939158', '26.236606&36.468896', '26.161624&43.656722', '19.113429&42.167148', '20.466868&45.562939', '21.207596&41.62114', '21.207596&41.62114', '27.345747&35.724307', '24.56457&46.849072', '28.310685&46.123127', '25.433633&49.627358', '25.378309&49.668169', '19.128444&41.924608', '26.857064&44.16728', '26.01725&43.358428', '26.651955&49.912345', '25.180349&44.611351', '25.180349&44.611351', '16.701895&42.120984', '16.701895&42.120984', '26.501759&45.34634', '26.959771&49.568742', '25.415248&49.679652', '27.266857&48.42877', '25.377098&49.72433', '30.114723&40.380291', '25.713421&45.870706', '21.565593&39.142663', '16.583613&42.91493', '22.74176&41.589691', '27.942469&48.661448', '21.357765&40.278224', '25.393521&49.729438', '17.460251&42.558314', '25.482034&49.554733', '16.826313&42.734487', '17.733627&42.265828', '17.733627&42.265828', '27.266857&48.42877', '26.443469&43.254569', '19.996937&42.226378', '30.99000633&41.02068966', '31.33330205&37.33329653', '24.09427736&38.0492948', '26.36638674&43.96283565', '24.07371014&45.28063635', '25.42905377&49.5659045', '20.00868695&42.59868119', '27.52357709&41.70007971', '16.90655072&42.5565649', '21.26222801&40.38227901', '17.50653994&44.1315592', '26.23241559&36.4635518']

        geocodes = list(set(geocodes))

        for i in geocodes:
            lat = i.split('&')[0]
            lon = i.split('&')[1]

            url = f'https://jahez.net/eRestaurant-services/m_getNearestRestaurant?callback=j&cuisineid=0&lat={lat}&lon={lon}&count=1&offset=0&lang=en'
            
            response = requests.request("GET", url)
            
            item = dict()
            
            if response.text.startswith('j('):
                jsonData = json.loads(response.text[2:-1])
            elif response.text.startswith('j{'):
                jsonData = json.loads(response.text[1:])
            else:
                jsonData = json.loads(response.text)
            
            for count, i in enumerate(jsonData['restaurantlist'], start=1):
                restaurantID = i['restaurantId']
                branchID = i['branchId']
                item['Restaurant'] = i['restaurantName']
                item['URL'] = f'https://jahez.net/eRestaurant-services/m_getMenuMobileListCombo-NMC-branch?restaurantId={restaurantID}&branchId={branchID}&categoryId=0&hasimage=Y&lang=en&menuclass=JAHEZ'
                
                try:
                    self.links.insertItemToSql(item)
                    print(f"{count}. Inserted - {item['URL']}")
                except Exception as e:
                    with open(f'{os.getcwd()}\Log\{date.today().strftime("%Y%m%d")}\{date.today().strftime("%Y%m%d")}.txt', 'a', encoding='utf_8') as f:
                        f.write(f'{e} during insertion in {count}. {item["Restaurant"]}\n\n')
                        print(f'{e} during insertion in {count}. {item["Restaurant"]}')

        print('\n\nLinks extracted successfully!\n')

    def dataExtraction(self):
        for link in self.links.fetchResultsfromSql(limit=25):
            item = dict()
            
            item['URL'] = link['URL']
            item['Restaurant'] = link['Restaurant']
            
            response = requests.request("GET", item['URL'])
            jsonData = json.loads(response.text)
            
            try:
                for i in jsonData['menuMobileList']:
                    item['Item'] = i['itemName']
                    item['Category'] = i['categoryNameEn']
                    
                    item['ImageURL'] = i['imageUrl']
                    if not item['ImageURL']:
                        item['ImageURL'] = ''
                    
                    item['Description'] = i['description']
                    if not item['Description']:
                        item['Description'] = ''

                    item['Price'] = i['prize']
                    
                    key = item['Item'] + item['Category'] + item['Restaurant'] + item['URL']
                    item['Hash'] = hashlib.md5(key.encode()).hexdigest()

                    self.data.insertItemToSql(item)
                    print(f'\n{item}')

                self.links.updateStatusSql(item)
            
            except Exception as e:
                with open(f'{os.getcwd()}\Log\{date.today().strftime("%Y%m%d")}\{date.today().strftime("%Y%m%d")}.txt', 'a', encoding='utf_8') as f:
                    f.write(f'\n{e} during injecting {item}\n\n')

    def exporttoCSV(self):
        query = f'select * from {self.data.table}'
        df = pandas.read_sql(query, self.data.connection)
        del df['Hash']
        del df['URL']
        df.to_csv(f'{os.getcwd()}\\Files\\{date.today().strftime("%Y%m%d")}\\Jahez_KSA_{date.today().strftime("%Y%m%d")}.csv', encoding='utf-8', index=False)
        print('\nExport Successfull!')
        print(f'Check Jahez_KSA_{date.today().strftime("%Y%m%d")}.csv in {os.getcwd()}\\Files\\{date.today().strftime("%Y%m%d")}\n')


if __name__ == '__main__':
    run = Scrapping()
    print("Initializing extraction. Please wait!\n")
    time.sleep(2)
    print("Starting part 1...\n")
    time.sleep(2)
    run.linksExtraction()
    print("Starting part 2...\n")
    time.sleep(2)
    run.dataExtraction()
    print('\n\nData extracted successfully!')
    print("\nStarting export. Kindly wait!\n")
    time.sleep(2)
    run.exporttoCSV()
    print("\n\nProcess Finished!")
    input("\nPress a key to exit...")