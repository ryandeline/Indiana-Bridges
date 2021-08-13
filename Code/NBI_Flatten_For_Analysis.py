import pandas as pd 
import numpy as np
import re
import pyodbc
from tqdm import tqdm

woutfile = "F:/Tableau/Transportation/Bridge Condition/table/_BridgeCleaning_TempOut/BridgeDashOut_tmp.csv"
outfile = "F:/Tableau/Transportation/Bridge Condition/table/_BridgeCleaning_TempOut/BridgeDashOut_tmp-JOIN.csv"

### Connect to Indiana Bridges DB
conn_str = (
	r'Driver={ODBC Driver 17 for SQL Server};'
	r'Server=T7500WIN10JH;'
	r'Database=Indiana_Bridges;'
	r'Trusted_Connection=yes;')

cnxn = pyodbc.connect(conn_str)

### Set sql Command

# cursor = cnxn.cursor()
# cursor.execute(sql)

### Verify output in terminal

# for row in cursor:
# 	print(row)

### Convert database streams into pandas dataframe
sql = """Select * FROM Indiana_Bridges.dbo.NBI"""
df = pd.read_sql(sql, cnxn)
# df.set_index('STRUCTURE_NUMBER_008')

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
				'Latitude', 'Longitude'
				], axis=1)

### Function Classification Decoded
sql2 = """Select * FROM Indiana_Bridges.dbo.FUNCTIONAL_CLASS_026"""
df2 = pd.read_sql(sql2, cnxn)
df['FUNCTIONAL_CLASS_026'] = df['FUNCTIONAL_CLASS_026'].map(df2.set_index('Code')['Description'])
df2.drop(df2.index, inplace = True)

### County Decoded
sql3 = """Select * FROM Indiana_Bridges.dbo.COUNTY_CODE_003"""
df3 = pd.read_sql(sql3, cnxn)
df['COUNTY_CODE_003'] = df['COUNTY_CODE_003'].map(df3.set_index('Code')['County'])
df3.drop(df3.index, inplace = True)

### Maintenance Responsibility
sql4 = """Select * FROM Indiana_Bridges.dbo.MAINTENANCE_021"""
df4 = pd.read_sql(sql4, cnxn)
df['MAINTENANCE_021'] = df['MAINTENANCE_021'].map(df4.set_index('Code')['Description'])
df4.drop(df4.index, inplace = True)

### Owner
sql5 = """Select * FROM Indiana_Bridges.dbo.OWNER_022"""
df5 = pd.read_sql(sql5, cnxn)
df['OWNER_022'] = df ['OWNER_022'].map(df5.set_index('Code')['Description'])
df5.drop(df5.index, inplace = True)

# ### Design Load
sql6 = """Select * FROM Indiana_Bridges.dbo.DESIGN_LOAD_031"""
df6 = pd.read_sql(sql6, cnxn)
df['DESIGN_LOAD_031'] = df['DESIGN_LOAD_031'].map(df6.set_index('Code')['Description'])
df6.drop(df6.index, inplace = True)

# ### Historical Significance
sql7 = """Select * FROM Indiana_Bridges.dbo.HISTORY_037"""
df7 = pd.read_sql(sql7, cnxn)
df['HISTORY_037'] = df['HISTORY_037'].map(df7.set_index('Code')['Description'])
df7.drop(df7.index, inplace = True)

# ### Structure Posting
sql8 = """Select * FROM Indiana_Bridges.dbo.OPEN_CLOSED_POSTED_041"""
df8 = pd.read_sql(sql8, cnxn)
df['OPEN_CLOSED_POSTED_041'] = df['OPEN_CLOSED_POSTED_041'].map(df8.set_index('Code')['Description'])
df8.drop(df8.index, inplace = True)

# ### Service on Bridge
sql9 = """Select * FROM Indiana_Bridges.dbo.SERVICE_ON_042A"""
df9 = pd.read_sql(sql9, cnxn)
df['SERVICE_ON_042A'] = df['SERVICE_ON_042A'].map(df9.set_index('Code')['Description'])
df9.drop(df9.index, inplace = True)

# ### Service under Bridge
sql10 = """Select * FROM Indiana_Bridges.dbo.SERVICE_UND_042B"""
df10 = pd.read_sql(sql10, cnxn)
df['SERVICE_UND_042B'] = df['SERVICE_UND_042B'].map(df10.set_index('Code')['Description'])
df10.drop(df10.index, inplace = True)

# ### Structure Kind
sql11 = """Select * FROM Indiana_Bridges.dbo.STRUCTURE_KIND_043A"""
df11 = pd.read_sql(sql11, cnxn)
df['STRUCTURE_KIND_043A'] = df['STRUCTURE_KIND_043A'].map(df11.set_index('Code')['Description'])
df11.drop(df11.index, inplace = True)

# ### Structure Type
sql12 = """Select * FROM Indiana_Bridges.dbo.STRUCTURE_TYPE_043B"""
df12 = pd.read_sql(sql12, cnxn)
df['STRUCTURE_TYPE_043B'] = df['STRUCTURE_TYPE_043B'].map(df12.set_index('Code')['Description'])
df12.drop(df12.index, inplace = True)

# ###  Approach Kind
sql13 = """Select * FROM Indiana_Bridges.dbo.APPR_KIND_044A"""
df13 = pd.read_sql(sql13, cnxn)
df['APPR_KIND_044A'] = df['APPR_KIND_044A'].map(df13.set_index('Code')['Description'])
df13.drop(df13.index, inplace = True)

# ### Approch Type
sql14 = """Select * FROM Indiana_Bridges.dbo.APPR_TYPE_044B"""
df14 = pd.read_sql(sql14, cnxn)
df['APPR_TYPE_044B'] = df['APPR_TYPE_044B'].map(df14.set_index('Code')['Description'])
df14.drop(df14.index, inplace = True)

# ### Deck Condition
sql15 = """Select * FROM Indiana_Bridges.dbo.DECK_COND_058"""
df15 = pd.read_sql(sql15, cnxn)
df['DECK_COND_058'] = df['DECK_COND_058'].map(df15.set_index('Code')['Description'])
df15.drop(df15.index, inplace = True)

# ### Superstructure Condition
sql16 = """Select * FROM Indiana_Bridges.dbo.SUPERSTRUCTURE_COND_059"""
df16 = pd.read_sql(sql16, cnxn)
df['SUPERSTRUCTURE_COND_059'] = df['SUPERSTRUCTURE_COND_059'].map(df16.set_index('Code')['Description'])
df16.drop(df16.index, inplace = True)

# ### Substructure Condition
sql17 = """Select * FROM Indiana_Bridges.dbo.SUBSTRUCTURE_COND_060"""
df17 = pd.read_sql(sql17, cnxn)
df['SUBSTRUCTURE_COND_060'] = df['SUBSTRUCTURE_COND_060'].map(df17.set_index('Code')['Description'])
df17.drop(df17.index, inplace = True)

# ### Channel Condition
sql18 = """Select * FROM Indiana_Bridges.dbo.CHANNEL_COND_061"""
df18 = pd.read_sql(sql18, cnxn)
df['CHANNEL_COND_061'] = df['CHANNEL_COND_061'].map(df18.set_index('Code')['Description'])
df18.drop(df18.index, inplace = True)

# ### Culvert Condition
sql19 = """Select * FROM Indiana_Bridges.dbo.CULVERT_COND_062"""
df19 = pd.read_sql(sql19, cnxn)
df['CULVERT_COND_062'] = df['CULVERT_COND_062'].map(df19.set_index('Code')['Description'])
df19.drop(df19.index, inplace = True)

# ### Operating Rating Method
sql20 = """Select * FROM Indiana_Bridges.dbo.OPR_RATING_METH_063"""
df20 = pd.read_sql(sql20, cnxn)
df['OPR_RATING_METH_063'] = df['OPR_RATING_METH_063'].map(df20.set_index('Code')['Description'])
df20.drop(df20.index, inplace = True)

# ### Inventory Rating Method
sql21 = """Select * FROM Indiana_Bridges.dbo.INV_RATING_METH_065"""
df21 = pd.read_sql(sql21, cnxn)
df['INV_RATING_METH_065'] = df['INV_RATING_METH_065'].map(df21.set_index('Code')['Description'])
df21.drop(df21.index, inplace = True)

# ### Structural Evaluation
sql22 = """Select * FROM Indiana_Bridges.dbo.STRUCTURAL_EVAL_067"""
df22 = pd.read_sql(sql22, cnxn)
df['STRUCTURAL_EVAL_067'] = df['STRUCTURAL_EVAL_067'].map(df22.set_index('Code')['Description'])
df22.drop(df22.index, inplace = True)

# ### Deck Geometry Evaluation
sql23 = """Select * FROM Indiana_Bridges.dbo.DECK_GEOMETRY_EVAL_068"""
df23 = pd.read_sql(sql23, cnxn)
df['DECK_GEOMETRY_EVAL_068'] = df['DECK_GEOMETRY_EVAL_068'].map(df23.set_index('Code')['Description'])
df23.drop(df23.index, inplace = True)

# ### Underclearance Evaluation
sql24 = """Select * FROM Indiana_Bridges.dbo.UNDCLRENCE_EVAL_069"""
df24 = pd.read_sql(sql24, cnxn)
df['UNDCLRENCE_EVAL_069'] = df['UNDCLRENCE_EVAL_069'].map(df24.set_index('Code')['Description'])
df24.drop(df24.index, inplace = True)

# ### Posting Evaluation
sql25 = """Select * FROM Indiana_Bridges.dbo.POSTING_EVAL_070"""
df25 = pd.read_sql(sql25, cnxn)
df['POSTING_EVAL_070'] = df['POSTING_EVAL_070'].map(df25.set_index('Code')['Description'])
df25.drop(df25.index, inplace = True)

# ### Waterway Evaluation
sql26 = """Select * FROM Indiana_Bridges.dbo.WATERWAY_EVAL_071"""
df26 = pd.read_sql(sql26, cnxn)
df['WATERWAY_EVAL_071'] = df['WATERWAY_EVAL_071'].map(df26.set_index('Code')['Description'])
df26.drop(df26.index, inplace = True)

# ### Approach Road Evaluation
sql27 = """Select * FROM Indiana_Bridges.dbo.APPR_ROAD_EVAL_072"""
df27 = pd.read_sql(sql27, cnxn)
df['APPR_ROAD_EVAL_072'] = df['APPR_ROAD_EVAL_072'].map(df27.set_index('Code')['Description'])
df27.drop(df27.index, inplace = True)

# ### Work Proposed
sql28 = """Select * FROM Indiana_Bridges.dbo.WORK_PROPOSED_075A"""
df28 = pd.read_sql(sql28, cnxn)
df['WORK_PROPOSED_075A'] = df['WORK_PROPOSED_075A'].map(df28.set_index('Code')['Description'])
df28.drop(df28.index, inplace = True)

# ### Work Done by
sql29 = """Select * FROM Indiana_Bridges.dbo.WORK_DONE_BY_075B"""
df29 = pd.read_sql(sql29, cnxn)
df['WORK_DONE_BY_075B'] = df['WORK_DONE_BY_075B'].map(df29.set_index('Code')['Description'])
df29.drop(df29.index, inplace = True)

# ### Federal Lands
sql30 = """Select * FROM Indiana_Bridges.dbo.FEDERAL_LANDS_105"""
df30 = pd.read_sql(sql30, cnxn)
df['FEDERAL_LANDS_105'] = df['FEDERAL_LANDS_105'].map(df30.set_index('Code')['Description'])
df30.drop(df30.index, inplace = True)

# ### Deck Material
sql31 = """Select * FROM Indiana_Bridges.dbo.DECK_STRUCTURE_TYPE_107"""
df31 = pd.read_sql(sql31, cnxn)
df['DECK_STRUCTURE_TYPE_107'] = df['DECK_STRUCTURE_TYPE_107'].map(df31.set_index('Code')['Description'])
df31.drop(df31.index, inplace = True)

# ### Wearing Surface
sql32 = """Select * FROM Indiana_Bridges.dbo.SURFACE_TYPE_108A"""
df32 = pd.read_sql(sql32, cnxn)
df['SURFACE_TYPE_108A'] = df['SURFACE_TYPE_108A'].map(df32.set_index('Code')['Description'])
df32.drop(df32.index, inplace = True)

# ### Membrane Type
sql33 = """Select * FROM Indiana_Bridges.dbo.MEMBRANE_TYPE_108B"""
df33 = pd.read_sql(sql33, cnxn)
df['MEMBRANE_TYPE_108B'] = df['MEMBRANE_TYPE_108B'].map(df33.set_index('Code')['Description'])
df33.drop(df33.index, inplace = True)

# ### Deck Protection
sql34 = """Select * FROM Indiana_Bridges.dbo.DECK_PROTECTION_108C"""
df34 = pd.read_sql(sql34, cnxn)
df['DECK_PROTECTION_108C'] = df['DECK_PROTECTION_108C'].map(df34.set_index('Code')['Description'])
df34.drop(df34.index, inplace = True)

# ### National Network
sql35 = """Select * FROM Indiana_Bridges.dbo.NATIONAL_NETWORK_110"""
df35 = pd.read_sql(sql35, cnxn)
df['NATIONAL_NETWORK_110'] = df['NATIONAL_NETWORK_110'].map(df35.set_index('Code')['Description'])
df35.drop(df35.index, inplace = True)

# ### Scour Critical Bridges
sql36 = """Select * FROM Indiana_Bridges.dbo.SCOUR_CRITICAL_113"""
df36 = pd.read_sql(sql36, cnxn)
df['SCOUR_CRITICAL_113'] = df['SCOUR_CRITICAL_113'].map(df36.set_index('Code')['Description'])
df36.drop(df36.index, inplace = True)

sql37 = """Select * FROM Indiana_Bridges.dbo.BridgeGeography"""
df37 = pd.read_sql(sql37, cnxn)
df['Latitude1'] = df37.apply(lambda x: x['Latitude']
	if x['Latitude'] == "" else x['Latitude'], axis = 1)
df['Latitude'] = df['Latitude1']

# df.join(df37.set_index('NBI_NUMBER'), on = 'STRUCTURE_NUMBER_008')

# df.set_index('STRUCTURE_NUMBER_008').join(df37.set_index('NBI_NUMBER'), how = 'left')

# df.join(df37, how = 'left')
df['Latitude'] = df['Latitude'].map(df37.set_index('NBI_NUMBER')['Latitude'])
# print(df['STRUCTURE_NUMBER_008']['Latitude'].head(5))

df = df.head(50)
df.to_csv(outfile, sep = ',')

df = df.rename(columns = {'STRUCTURE_NUMBER_008':'NBI Number', 'Year':'Rating Year', 'Latitude':'Latitude',
							'Longitude':'Longitude', 'DATE_OF_INSPECT_090':'Inspection Date', 'INSPECT_FREQ_MONTHS_091':'Inspection Frequency',
							'YEAR_BUILT_027':'Year Built', 'YEAR_RECONSTRUCTED_106':'Year Reconstructed',
							'INDOT_District':'INDOT District', 'MPO':'MPO', 'COUNTY_CODE_003':'County', 'OWNER_022':'Owner', 'MAINTENANCE_021':'Maintenance Responsibility',
							'FACILITY_CARRIED_007':'Facility Carried', 'FEATURES_DESC_006A':'Feature Intersected',
							'Place':'Place', 'OPEN_CLOSED_POSTED_041':'Structure Posting', 'ADT_029':'Inspection ADT', 'YEAR_ADT_030':'ADT Year',
							'DECK_COND_058':'Deck', 'SUPERSTRUCTURE_COND_059':'Superstructure', 'SUBSTRUCTURE_COND_060':'Substructure',
							'CULVERT_COND_062':'Culverts', 'CHANNEL_COND_061':'Channel Condition', 'STRUCTURAL_EVAL_067':'NBI Condition',
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
							'TEMP_STRUCTURE_103':'TEMP_STRUCTURE_103', 'HIGHWAY_SYSTEM_104':'HIGHWAY_SYSTEM_104', 'FEDERAL_LANDS_105':'FEDERAL_LANDS_105',
							'SURFACE_TYPE_108A':'SURFACE_TYPE_108A', 'MEMBRANE_TYPE_108B':'MEMBRANE_TYPE_108B', 'DECK_PROTECTION_108C':'DECK_PROTECTION_108C',
							'PERCENT_ADT_TRUCK_109':'PERCENT_ADT_TRUCK_109', 'NATIONAL_NETWORK_110':'NATIONAL_NETWORK_110'})


### Output to Master Bridge Analysis Record
df = df.head(50)
df.to_csv(woutfile, sep = ',')
tqdm.pandas()