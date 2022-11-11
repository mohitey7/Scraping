import pymysql, pymongo

class ConfigDatabase():
    def __init__(self, database, table, host="localhost", user="root", password="actowiz", type1 = "sql"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = table

        self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.connection.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")

        if type1 == 'mongo':
            self.connMongo = pymongo.MongoClient(f"mongodb://{host}:27017/")
            self.dbmongo = self.connMongo[self.database]
        self.crsrSql = self.connection.cursor(pymysql.cursors.DictCursor)
    
    def insertItemToSql(self, item):
        try:
            field_list = []
            value_list = []
            for field in item:
                field_list.append(str(field))
                value_list.append(str(item[field]).replace("'", "’"))
            fields = ','.join(field_list)
            values = "','".join(value_list)
            insert_db = f"insert into {self.table}" + "( " + fields + " ) values ( '" + values + "' )"
            self.crsrSql.execute(insert_db)
            self.connection.commit()
            print(f"Item inserted!")
            
            try:
                update = f"update links set Status = 'Done' where URL = '{item['product_url']}'"
                self.crsrSql.execute(update)
                self.connection.commit()
            except Exception as e:
                print(f'{e} during status update!')
        
        except Exception as e:
            print(str(e))
    
    def updateStatusSql(self, item,):
        try:
            update = f"update links set Status = 'Done' where URL = '{item['product_url']}'"
            self.crsrSql.execute(update)
            self.connection.commit()
        except Exception as e:
            print(f'{e} during status update!')