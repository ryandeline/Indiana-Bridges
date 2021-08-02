#
#
#  	Indiana Bridge Cleaning of Original NBI database pulls.  Clean_1.py
#	Clean NBI data and push them to mssql 2019.
#
#	Created by Ryan DeLine on 05/06/2021
#
#	

import pandas as pd
import numpy as np

winfile = "X:/Users/rdeline/Documents/GitHub/Indiana Bridge/NBI_Files/NBI_2020.csv"
### Database Connection
woutfile = "X:/Users/rdeline/Documents/GitHub/Indiana Bridge/NBI_Files/Clean/NBI_2020c.csv"
codefile = "X:/Users/rdeline/Documents/GitHub/Indiana Bridge/Column_Tables/Coding.csv"

df = pd.read_csv(winfile, low_memory = False, encoding = "ISO-8859-1")
df2 = pd.read_csv(codefile)

### Drop columns which are not consistant year to year.
df = df.drop(['HIGHWAY_DISTRICT_002', 'PLACE_CODE_004', 'LAT_016', 'LONG_017', 'FED_AGENCY', 'SUBMITTED_BY', 'BRIDGE_CONDITION', 'LOWEST_RATING', 'DECK_AREA'], axis = 1)

### Join Code file df2 with annual NBI file
df['key'] = df['STRUCTURE_NUMBER_008']
df3 = df.set_index('key').join(df2.set_index('NBI Number'))

# print(df['FEATURES_DESC_006A'].dtypes)

### Remove apostrophe's and white space from colums
df3['FEATURES_DESC_006A'] = df3['FEATURES_DESC_006A'].astype(str)
df3['FEATURES_DESC_006A'] = df3['FEATURES_DESC_006A'].map(lambda x: x.lstrip("'").rstrip("'"))
df3['FEATURES_DESC_006A'] = df3['FEATURES_DESC_006A'].map(lambda x: x.rstrip(" "))
df3['FACILITY_CARRIED_007'] = df3['FACILITY_CARRIED_007'].astype(str)
df3['FACILITY_CARRIED_007'] = df3['FACILITY_CARRIED_007'].map(lambda x: x.lstrip("'").rstrip("'"))
df3['FACILITY_CARRIED_007'] = df3['FACILITY_CARRIED_007'].map(lambda x: x.rstrip(" "))
df3['STRUCTURE_NUMBER_008'] = df3['STRUCTURE_NUMBER_008'].map(lambda x: x.lstrip(" ").rstrip(" "))
df3['LOCATION_009'] = df3['LOCATION_009'].astype(str)
df3['LOCATION_009'] = df3['LOCATION_009'].map(lambda x: x.lstrip("'").rstrip("'"))
df3['LOCATION_009'] = df3['LOCATION_009'].map(lambda x: x.rstrip(" "))

# df3 = df3.reset_index(drop = True, inplace = True)

# print(df3)

df = df3

### Order columns to fit sql schema
df = df[['STRUCTURE_NUMBER_008', 'YEAR', 'STATE_CODE_001', 'RECORD_TYPE_005A', 'ROUTE_PREFIX_005B', 'SERVICE_LEVEL_005C', 'ROUTE_NUMBER_005D', 'DIRECTION_005E', 'Place (Decoded)',
		'COUNTY_CODE_003', 'INDOT District', 'Lattitude', 'Longitude', 'FEATURES_DESC_006A', 'CRITICAL_FACILITY_006B', 'FACILITY_CARRIED_007', 'LOCATION_009', 'MIN_VERT_CLR_010', 
		'KILOPOINT_011', 'BASE_HWY_NETWORK_012', 'LRS_INV_ROUTE_013A', 'SUBROUTE_NO_013B', 'DETOUR_KILOS_019', 'TOLL_020', 'MAINTENANCE_021', 'OWNER_022', 'FUNCTIONAL_CLASS_026', 
		'YEAR_BUILT_027', 'TRAFFIC_LANES_ON_028A', 'TRAFFIC_LANES_UND_028B', 'ADT_029', 'YEAR_ADT_030', 'DESIGN_LOAD_031', 'APPR_WIDTH_MT_032', 'MEDIAN_CODE_033', 'DEGREES_SKEW_034', 
		'STRUCTURE_FLARED_035', 'RAILINGS_036A', 'TRANSITIONS_036B', 'APPR_RAIL_036C', 'APPR_RAIL_END_036D', 'HISTORY_037', 'NAVIGATION_038', 'NAV_VERT_CLR_MT_039', 'NAV_HORR_CLR_MT_040', 
		'OPEN_CLOSED_POSTED_041', 'SERVICE_ON_042A', 'SERVICE_UND_042B', 'STRUCTURE_KIND_043A', 'STRUCTURE_TYPE_043B', 'APPR_KIND_044A', 'APPR_TYPE_044B', 'MAIN_UNIT_SPANS_045', 
		'APPR_SPANS_046', 'HORR_CLR_MT_047', 'MAX_SPAN_LEN_MT_048', 'STRUCTURE_LEN_MT_049', 'LEFT_CURB_MT_050A', 'RIGHT_CURB_MT_050B', 'ROADWAY_WIDTH_MT_051', 'DECK_WIDTH_MT_052', 
		'VERT_CLR_OVER_MT_053', 'VERT_CLR_UND_REF_054A', 'VERT_CLR_UND_054B', 'LAT_UND_REF_055A', 'LAT_UND_MT_055B', 'LEFT_LAT_UND_MT_056', 'DECK_COND_058', 'SUPERSTRUCTURE_COND_059', 
		'SUBSTRUCTURE_COND_060', 'CHANNEL_COND_061', 'CULVERT_COND_062', 'OPR_RATING_METH_063', 'OPERATING_RATING_064', 'INV_RATING_METH_065', 'INVENTORY_RATING_066', 'STRUCTURAL_EVAL_067', 
		'DECK_GEOMETRY_EVAL_068', 'UNDCLRENCE_EVAL_069', 'POSTING_EVAL_070', 'WATERWAY_EVAL_071', 'APPR_ROAD_EVAL_072', 'WORK_PROPOSED_075A', 'WORK_DONE_BY_075B', 'IMP_LEN_MT_076', 
		'DATE_OF_INSPECT_090', 'INSPECT_FREQ_MONTHS_091', 'FRACTURE_092A', 'UNDWATER_LOOK_SEE_092B', 'SPEC_INSPECT_092C', 'FRACTURE_LAST_DATE_093A', 'UNDWATER_LAST_DATE_093B', 
		'SPEC_LAST_DATE_093C', 'BRIDGE_IMP_COST_094',	'ROADWAY_IMP_COST_095', 'TOTAL_IMP_COST_096', 'YEAR_OF_IMP_097', 'OTHER_STATE_CODE_098A', 'OTHER_STATE_PCNT_098B', 
		'OTHR_STATE_STRUC_NO_099', 'STRAHNET_HIGHWAY_100', 'PARALLEL_STRUCTURE_101', 'TRAFFIC_DIRECTION_102', 'TEMP_STRUCTURE_103', 'HIGHWAY_SYSTEM_104', 'FEDERAL_LANDS_105', 
		'YEAR_RECONSTRUCTED_106', 'DECK_STRUCTURE_TYPE_107', 'SURFACE_TYPE_108A', 'MEMBRANE_TYPE_108B', 'DECK_PROTECTION_108C', 'PERCENT_ADT_TRUCK_109', 'NATIONAL_NETWORK_110', 
		'PIER_PROTECTION_111', 'BRIDGE_LEN_IND_112', 'SCOUR_CRITICAL_113', 'FUTURE_ADT_114', 'YEAR_OF_FUTURE_ADT_115', 'MIN_NAV_CLR_MT_116']]

print(df)

df.to_csv(woutfile, sep = ',')