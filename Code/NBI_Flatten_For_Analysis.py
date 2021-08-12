import pandas as pd 
import numpy as np
import re
import pyodbc
from tqdm import tqdm

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
sql2 = """Select * FROM Indiana_Bridges.dbo.FUNCTIONAL_CLASS_026"""
sql3 = """Select * FROM Indiana_Bridges.dbo.COUNTY_CODE_003"""
sql4 = """Select * FROM Indiana_Bridges.dbo.MAINTENANCE_021"""
sql5 = """Select * FROM Indiana_Bridges.dbo.OWNER_022"""
# cursor.execute(sql)

### Verify output in terminal

# for row in cursor:
# 	print(row)

### Convert database streams into pandas dataframe

df = pd.read_sql(sql, cnxn)
df2 = pd.read_sql(sql2, cnxn)
df3 = pd.read_sql(sql3, cnxn)
df4 = pd.read_sql(sql4, cnxn)
df5 = pd.read_sql(sql5, cnxn)

### Begining Cleaning Analyst Output
df = df.drop(['STATE_CODE_001', 'RECORD_TYPE_005A', 'ROUTE_PREFIX_005B', 'SERVICE_LEVEL_005C',
				'ROUTE_NUMBER_005D', 'CRITICAL_FACILITY_006B', 'MIN_VERT_CLR_010', 'KILOPOINT_011',
				'BASE_HWY_NETWORK_012', 'LRS_INV_ROUTE_013A', 'SUBROUTE_NO_013B', 'DEGREES_SKEW_034',
				'STRUCTURE_FLARED_035', 'NAVIGATION_038', 'NAV_VERT_CLR_MT_039', 'NAV_HORR_CLR_MT_040',
				'LEFT_CURB_MT_050A', 'RIGHT_CURB_MT_050B', 'LAT_UND_REF_055A', 'LAT_UND_MT_055B', 
				'LEFT_LAT_UND_MT_056', 'IMP_LEN_MT_076', 'BRIDGE_IMP_COST_094', 'ROADWAY_IMP_COST_095',
				'OTHER_STATE_CODE_098A', 'OTHER_STATE_PCNT_098B', 'OTHR_STATE_STRUC_NO_099', 
				'PARALLEL_STRUCTURE_101', 'TRAFFIC_DIRECTION_102', 'PIER_PROTECTION_111', 'BRIDGE_LEN_IND_112',
				'FUTURE_ADT_114', 'YEAR_OF_FUTURE_ADT_115', 'MIN_NAV_CLR_MT_116', 'DIRECTION_005E', 'TOLL_020',
				], axis=1)

### Function Classification Decoded

df['FUNCTIONAL_CLASS_026'] = df['FUNCTIONAL_CLASS_026'].map(df2.set_index('Code')['Description'])

### County Decoded

df['COUNTY_CODE_003'] = df['COUNTY_CODE_003'].map(df3.set_index('Code')['County'])

### Maintenance Responsibility

df['MAINTENANCE_021'] = df['MAINTENANCE_021'].map(df4.set_index('Code')['Description'])

### Owner

df['OWNER_022'] = df ['OWNER_022'].map(df5.set_index('Code')['Description'])

### Design Load

df['DESIGN_LOAD_031']

### Historical Significance

df['']

df = df.rename(columns = {'STRUCTURE_NUMBER_008':'NBI Number', 'Year':'Rating Year', 'Latitude':'Latitude',
							'Longitude':'Longitude', 'DATE_OF_INSPECT_090':'Inspection Date', 'INSPECT_FREQ_MONTHS_091':'Inspection Frequency',
							'YEAR_BUILT_027':'Year Built', 'YEAR_RECONSTRUCTED_106':'Year Reconstructed',
							'INDOT_District':'INDOT District', 'MPO':'MPO', 'COUNTY_CODE_003':'County', 'OWNER_022':'Owner', 'MAINTENANCE_021':'Maintenance Responsibility',
							'FACILITY_CARRIED_007':'Facility Carried', 'FEATURES_DESC_006A':'Feature Intersected',
							'Place':'Place', 'OPEN_CLOSED_POSTED_041':'Structure Posting', 'ADT_029':'Inspection ADT', 'YEAR_ADT_030':'ADT Year',
							'DECK_COND_058':'Deck', 'SUPERSTRUCTURE_COND_059':'Superstructure', 'SUBSTRUCTURE_COND_060':'Substructure',
							'CULVERT_COND_062':'Culverts', 'CHANNEL_COND_060':'Channel Condition', 'STUCTURAL_EVAL_067':'NBI Condition',
							'DETOUR_KILOS_019':'Detour (k)', 'FUNCTIONAL_CLASS_026':'Functional Classification',
							'HISTORY_037':'Historical Significance', 'TRAFFUC_LANES_ON_028A':'Lanes on Structure',
							'TRAFFIC_LANES_UND_028B':'Lanes Under Structure', 'APPR_WIDTH_MT_032':'Approach Width (m)',
							'MAX_SPAN_LEN_MT_048':'Maximum Span (m)', 'STRUCTURE_LEN_MT_049':'Structure Length (m)',
							'MAIN_UNIT_SPANS_045':'Number of Spans', 'OPERATING_RATING_064':'Operating Rating', 'OPR_RATING_METH_063':'Operating Rating Method',
							'INVENTORY_RATING_066':'Inventory Rating', 'INV_RATING_METH_065':'Inventory Rating Method',
							'LOCATION_009':'Location', 'STRUCTURE_TYPE_043B':'Structure Type', 'ROADWAY_WIDTH_MT_051':'Road Width (m)',
							'DECK_WIDTH_MT_052':'Deck Width (m)', 'VERT_CLR_OVER_MT_053':'Vertical Clearance Above Bridge (m)',
							'VERT_CLR_UND_REF_054A':'Vertical Underclearance Reference','VERT_CLR_UND_054B':'Vertical Underclearance (m)',
							'DECK_GEOMETRY_EVAL_068':'Deck Geometry Evaluation', 'UNDCLRENCE_EVAL_069':'Underclearance Evaluation',
							'POSTING_EVAL_070':'Posting Evaluation', 'WATERWAY_EVAL_071':'Waterway Evaluation', 'APPR_ROAD_EVAL_072':'Approach Road Evaluation',
							'DECK_STRUCTURE_TYPE_107':'Deck Material', 'FRACTURE_092A':'Fracture Critical Inspection Rate',
							'SCOUR_CRITICAL_113':'Scour Critical Bridges', 'DESIGN_LOAD_031':'Design Load', 'MEDIAN_CODE_033':'MEDIAN_CODE_033',
							'RAILINGS_036A':'RAILINGS_036A', 'TRANSITIONS_036B':'TRANSITIONS_036B', 'APPR_RAIL_036C':'APPR_RAIL_036C',
							'APPR_RAIL_END_036D':'APPR_RAIL_END_036D', 'SERVICE_ON_042A':'SERVICE_ON_042A', 'SERVICE_UND_042B':'SERVICE_UND_042B',
							'STRUCTURE_KIND_043A':'STRUCURE_KIND_043A', 'APPR_KIND_044A':'APPR_KIND_044A', 'APPR_TYPE_044B':'APPR_TYPE_044B',
							'APPR_SPANS_046':'APPR_SPANS_046', 'HORR_CLR_MT_047':'HORR_CLR_MT_047', 'WORK_PROPOSED_075A':'WORK_PROPOSED_075A',
							'WORK_DONE_BY_075B':'WORK_DONE_BY_075B', 'UNDERWATER_LOOK_SEE_092B':'UNDERWATER_LOOK_SEE_092B',
							'SPEC_INSPECT_092C':'SPEC_INSPEC_092C', 'FRACTURE_LAST_DATE_093A':'FRACTURE_LAST_DATE_093', 
							'UNDERWATER_LAST_DATE_093B':'UNDERWATER_LOOK_SEE_092B', 'SPEC_LAST_DATE_093C':'SPEC_LAST_DATE_093C',
							'TOTAL_IMP_COST_096':'TOTAL_IMP_COST_096','YEAR_OF_IMP_097':'YEAR_OF_IMP_097', 'STRAHNET_HIGHWAY_100':'STRAHNET_HIGHWAY_100',
							'TEMP_STRUCTURE_103':'TEMP_STRUCTURE_103', 'HIGHWAY_SYSTEM_104':'HIGHWAY_SYSTEM_104', 'FEDERAL_LANDS_105':'FEDERAL_LANDS_2015',
							'SURFACE_TYPE_108A':'SURFACE_TYPE_108A', 'MEMBRANE_TYPE_108B':'MEMBRANE_TYPE_108B', 'DECK_PROTECTION_108C':'DECK_PROTECTION_108C',
							'PERCENT_ADT_TRUCK_109':'PERCENT_ADT_TRUCK_109', 'NATIONAL_NETWORK_110':'NATIONAL_NETWORK_110'})


### Output to Master Bridge Analysis Record
df = df.head(5)
df.to_csv(woutfile, sep = ',')
tqdm.pandas()