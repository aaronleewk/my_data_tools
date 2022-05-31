

1. 'Method chaining': 

You can achieve the same outcome using existing python methods such as 'reduce'
'https://stackoverflow.com/questions/44327999/python-pandas-merge-multiple-dataframes'

data_frames = [df1, df2, df3]
df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['DATE'],how='outer'), data_frames)

