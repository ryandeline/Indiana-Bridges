import pandas as pd 

df = pd.read_csv('F:/Tableau/Bridge Condition/table/2010-2020_test2.csv', low_memory = False)
outfile = ('F:/Tableau/Bridge Condition/table/2010-2020_Structure_Count.csv')

@classmethod
class Duplicates:
	DUPLICATE_COUNTS = ('1', '2', '3')
	__duplist = None

	def getDups(cls):
		return cls.DUPLICATE_COUNTS

	def __init__(self, duplicates):
		if self.duplicates > 1:
			return 'Duplicate Value'
		else:
			self.duplicates = self.duplicates

df['STRUCTURE_NUMBER_008'] = df['STRUCTURE_NUMBER_008'].map(lambda x: x.strip(" "))
df['dups'] =+ 1
df2 = df.groupby(['Year', "STRUCTURE_NUMBER_008", 'dups']).size()

# if df2.size >= 2:
# 	df2['dups'] == 'Duplicate Value'
# else:
# 	df2['dups'] == 'Unique Value'

# df2["STRUCTURE_NUMBER_008"].strip()
print(df2)

df2.to_csv(outfile, sep =',')
# print(df)`