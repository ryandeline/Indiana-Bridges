#
#
#  	Indiana Bridge Cleaning of Original NBI database pulls.  Clean_1.py
#	Clean NBI data and push them to mssql 2019.
#
#	Created by Ryan DeLine on 05/06/2021
#
#	

import csv
import pandas as pd 
import numpy as np 
from tqdm import tqdm

winfile = "Y:/GitHub/Indiana-Bridges/NBI_Files/NBI_2010.csv"
### Database Connection
woutfile = "Y:/GitHub/Indiana-Bridges/NBI_Files/Clean/NBI_****.csv"
codefile = "Y:/GitHub/Indiana-Bridges/Column_Tables/Coding.csv"

df = pd.read_csv(winfile, Low_memory = False, encoding = "ISO-8859-1")
df2 = pd.read_csv(codefile)

### Drop columns which are not consistant year to year.
df = df.drop(['FED_AGENCY', 'DATE_LAST_UPDATE', 'TYPE_LAST_UPDATE', 'DEDUCT_CODE',	'REMARKS', 'PROGRAM_CODE',
					'PROJ_NO', 'PROJ_SUFFIX', 'NBI_TYPE_OF_IMP', 'DTL_TYPE_OF_IMP',	'STEP_CODE', 'STATUS_WITH_10YR_RULE',
					'SUFFICIENCY_ASTERC', 'SUFFICIENCY_RATING',	'STATUS_NO_10YR_RULE'])

df3 = pd.concat([df, df2], axis = 1)

print(df3)

df3.to_csv(woutfile, sep = ',')