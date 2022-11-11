import pymongo
import pymysql


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
        print(f"Item inserted!")
    
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