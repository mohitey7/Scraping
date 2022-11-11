import pandas as pd, numpy as np, pymysql
from pymongo import MongoClient

# client = MongoClient('localhost', 27017)
# db = client['amazonus']
# data = db['data2']
# data = list(data.find())
# df = pd.DataFrame(data)
# del df['_id']
connection = pymysql.connect(host='localhost', user='root', password='actowiz', database='jahez')
query = "select * from data_20221019"
df = pd.read_sql(query, connection)
df.to_excel(r'D:\Mohit_sharing\Jahez\Jahez_20221019.xlsx', index=False)
print('Done')