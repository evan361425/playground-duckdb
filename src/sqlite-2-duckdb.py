import pandas as pd
import sqlite3
import duckdb as duck

parquetFile = 'data/tables/prod.parquet'
conn = sqlite3.connect('data/prod.sqlite')
df = pd.read_sql('SELECT * from xray', conn)
df.to_parquet(parquetFile, index = False)

con = duck.connect(database='data/prod.db', read_only=False)
con.execute("CREATE TABLE xray AS SELECT * FROM parquet_scan('%s')" % parquetFile)
print(con.fetchall())
