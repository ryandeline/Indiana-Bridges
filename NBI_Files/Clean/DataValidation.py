import pandas as pd 

df = pd.read_csv('F:/Tableau/Bridge Condition/table/2010-2020_test2.csv', low_memory = False)
outfile = ('F:/Tableau/Bridge Condition/table/2010-2020_Structure_Count.csv')

class Duplicates:
	DUPLICATE_COUNTS = ('1', '2', '3')
	__duplist = None

	@classmethod
	def getDups(cls):
		return cls.DUPLICATE_COUNTS

	def __init__(self, duplicates):
		if df2['0'] > 1:
			return 'Duplicate Value'
		else:
			self.duplicates = df2['0']

df2 = df.groupby(['Year', "STRUCTURE_NUMBER_008"]).size()

df2.to_csv(outfile, sep =',')
# print(df)