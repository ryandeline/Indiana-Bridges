import pandas as pd 
import numpy as np
import pyodbc

woutfile = "X:/Users/rdeline/Documents/_BridgeCleaning_TempOut/BridgeDashOut_tmp.csv"

### Connect to Indiana Bridges DB

conn_str = (
	r'Driver={ODBC Driver 17 for SQL Server};'
	r'Server=T7500WIN10JH;'
	r'Database=Indiana_Bridges;'
	r'Trusted_Connection=yes;')

cnxn = pyodbc.connect(conn_str)

### Set sql Command

cursor = cnxn.cursor()
sql = """Select TOP 10 * FROM Indiana_Bridges.dbo.NBI"""
cursor.execute(sql)

### Verify output in terminal

for row in cursor:
	print(row)

### Convert database stream into pandas dataframe

df = pd.read_sql(sql, cnxn)
# print(df.head(5))




### Output to Master Bridge Analysis Record

df.to_csv(woutfile, sep = ',')