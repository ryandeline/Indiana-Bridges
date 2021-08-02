import pandas as pd 
import numpy as np
import pyodbc

woutfile = "F:/Tableau/Transportation/Bridge Condition/table/_BridgeCleaning_TempOut/BridgeDashOut_tmp.csv"

### Connect to Indiana Bridges DB

conn_str = (
	r'Driver={ODBC Driver 17 for SQL Server};'
	r'Server=T7500WIN10JH;'
	r'Database=Indiana_Bridges;'
	r'Trusted_Connection=yes;')

cnxn = pyodbc.connect(conn_str)

### Set sql Command

# cursor = cnxn.cursor()
sql = """Select * FROM Indiana_Bridges.dbo.NBI"""
# cursor.execute(sql)

### Verify output in terminal

# for row in cursor:
# 	print(row)

### Convert database stream into pandas dataframe

df = pd.read_sql(sql, cnxn)
# print(df.head(5))

### Begining Cleaning Analyst Output

df = df.drop(['STATE_CODE_001', 'RECORD_TYPE_005A', 'ROUTE_PREFIX_005B', 'SERVICE_LEVEL_005C',
				'ROUTE_NUMBER_005D', 'CRITICAL_FACILITY_006B', 'MIN_VERT_CLR_010', 'KILOPOINT_011',
				'BASE_HWY_NETWORK_012', 'LRS_INV_ROUTE_013A', 'SUBROUTE_NO_13B', 'DEGREES_SKEW_034',
				'STRUCTURE_FLARED_035', 'NAVIGATION_038', 'NAV_CLR_MT_039', 'NAV_HORR_CLR_MT_040',
				'LEFT_CURB_MT_050A', 'RIGHT_CURB_MT_050B', 'LAT_UND_REF_055A', 'LAT_UND_MT_055B', 
				'LEFT_LAT_UND_MT_056', 'IMP_LEN_MT_076', 'BRIDGE_IMP_COST_094', 'ROADWAY_IMP_COST_095',
				'OTHER_STATE_CODE_098A', 'OTHER_STATE_PCNT_098B', 'OTHER_STATE_STRUC_NO_099', 
				'PARALLEL_STRUCTURE_101', 'TRAFFIC_DIRECTION_102', 'PIER_PROTECTION_111', 'BRIDGE_LEN_IND_112',
				'FUTURE_ADT_114', 'YEAR_OF_FUTURE_ADT_115', 'MIN_NAV_CLR_MT_116'], axis=1)


### Output to Master Bridge Analysis Record

df.to_csv(woutfile, sep = ',')