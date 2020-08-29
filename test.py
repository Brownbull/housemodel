import pandas as pd

datasourcesPath = "D:\Reference\housemodel\data\ETL\E\\byHand\segment3_datasources.csv"
inDf = pd.read_csv(datasourcesPath)
for idx, row in inDf.iterrows():
  print(row['Link'])




print("end of test")