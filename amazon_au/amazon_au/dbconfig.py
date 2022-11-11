import pymysql

class ConfigSql():
# to create connection and database
    def __init__(self, database, table):
        self.host = 'localhost'
        self.user = 'root'
        self.password = 'workbench'
        self.table = table
        self.database = database
        self.initialconnection = pymysql.connect(host=self.host, user=self.user, password=self.password)
        self.initialconnection.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, database=f'{database}')
        self.cursor = self.connection.cursor()
        self.dictcursor = self.connection.cursor(pymysql.cursors.DictCursor)

    def fetch(self, start=0, end=1):
        query  = f"select * from {self.table} where Status = 'Pending'"
        self.dictcursor.execute(query)
        results = self.dictcursor.fetchall()
        return results

    def insert(self, item):
        fields = []
        values = []
        for field in item:
            fields.append(str(field))
            values.append(str(item[field]).replace("'", "’"))
        fields = ','.join(fields)
        values = "','".join(values)
        query = f"insert into {self.table}" + "(" + fields + ") values ('" + values + "')"
        self.cursor.execute(query)
        self.connection.commit()

        # for field in item:
        #         field_list.append(str(field))
        #         value_list.append(str(item[field]).replace("'", "’"))
        # fields = ','.join(field_list)
        # values = "','".join(value_list)
        # insert_db = f"insert into {self.table}" + "( " + fields + " ) values ( '" + values + "' )"
        # self.crsrSql.execute(insert_db)
        # self.connection.commit()

    def update(self, item):
        query = f"update links set Status = 'Done' where URL = '{item['URL']}'"
        self.cursor.execute(query)
        self.connection.commit()