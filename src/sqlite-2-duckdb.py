import pandas as pd
import sqlite3
import duckdb as duck

parquetFile = 'data/tables/xray.parquet'
conn = sqlite3.connect('data/poc.sqlite')
df = pd.read_sql('SELECT * from xray', conn)
df.to_parquet(parquetFile, index = False)

con = duck.connect(database='data/xray.db', read_only=False)
con.execute("CREATE TABLE xray AS  SELECT * FROM parquet_scan('%s')" % parquetFile)
print(con.fetchall())
