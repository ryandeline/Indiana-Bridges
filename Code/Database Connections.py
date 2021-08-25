import pandas as pd 
import numpy as np
import re
import pyodbc
from tqdm import tqdm

woutfile = "F:/Tableau/Transportation/Bridge Condition/table/_BridgeCleaning_TempOut/function_test.csv"
outfile = "F:/Tableau/Transportation/Bridge Condition/table/_BridgeCleaning_TempOut/BridgeDashOut_tmp-JOIN.csv"
testfile = "F:/Tableau/Transportation/Bridge Condition/table/_BridgeCleaning_TempOut/BridgeDashOut_tmp-test.csv"

### Connect to Indiana Bridges DB
conn_str = (
	r'Driver={ODBC Driver 17 for SQL Server};'
	r'Server=T7500WIN10JH;'
	r'Database=Indiana_Bridges;'
	r'Trusted_Connection=yes;')

# cnxn = pyodbc.connect(conn_str)

def execute_query(pyodbc, q):
	cnxn = pyodbc.connect(conn_str)
	result = None
	try:
		result = pd.read_sql(q, cnxn)
		return result
	except Error as e:
		print(f"the error '{e}' occurred")

sql = """Select * FROM Indiana_Bridges.dbo.FUNCTIONAL_CLASS_026"""
df = execute_query(pyodbc, sql)

df.to_csv(woutfile, sep = ',')

### Set sql Command

# cursor = cnxn.cursor()
# cursor.execute(sql)

### Verify output in terminal

# for row in cursor:
# 	print(row)

### Convert database streams into pandas dataframe

# df = pd.read_sql(sql, cnxn)
# df.set_index('STRUCTURE_NUMBER_008')

# Only keep the most recent inspection
# df = df.sort_values(by=['STRUCTURE_NUMBER_008', 'Year'])
# df = df.drop_duplicates(subset=['STRUCTURE_NUMBER_008'], keep='last')

### Begining Cleaning Analyst Output
# df = df.drop(['STATE_CODE_001', 'RECORD_TYPE_005A', 'ROUTE_PREFIX_005B', 'SERVICE_LEVEL_005C',
# 				'ROUTE_NUMBER_005D', 'CRITICAL_FACILITY_006B', 'MIN_VERT_CLR_010', 'KILOPOINT_011',
# 				'BASE_HWY_NETWORK_012', 'LRS_INV_ROUTE_013A', 'SUBROUTE_NO_013B', 'DEGREES_SKEW_034',
# 				'STRUCTURE_FLARED_035', 'NAVIGATION_038', 'NAV_VERT_CLR_MT_039', 'NAV_HORR_CLR_MT_040',
# 				'LEFT_CURB_MT_050A', 'RIGHT_CURB_MT_050B', 'LAT_UND_REF_055A', 'LAT_UND_MT_055B', 
# 				'LEFT_LAT_UND_MT_056', 'IMP_LEN_MT_076', 'BRIDGE_IMP_COST_094', 'ROADWAY_IMP_COST_095',
# 				'OTHER_STATE_CODE_098A', 'OTHER_STATE_PCNT_098B', 'OTHR_STATE_STRUC_NO_099', 
# 				'PARALLEL_STRUCTURE_101', 'TRAFFIC_DIRECTION_102', 'PIER_PROTECTION_111', 'BRIDGE_LEN_IND_112',
# 				'FUTURE_ADT_114', 'YEAR_OF_FUTURE_ADT_115', 'MIN_NAV_CLR_MT_116', 'DIRECTION_005E', 'TOLL_020'
# 				], axis=1)

# df = df.rename(columns = {'Latitude':'Latitude_NBI', 'Longitude':'Longitude_NBI'})


### Function Classification Decoded
# sql2 = """Select * FROM Indiana_Bridges.dbo.FUNCTIONAL_CLASS_026"""
# df2 = pd.read_sql(sql2, cnxn)
# df['FUNCTIONAL_CLASS_026'] = df['FUNCTIONAL_CLASS_026'].map(df2.set_index('Code')['Description'])
# df2.drop(df2.index, inplace = True)

# df.to_csv(woutfile, sep = ',')