# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  scrapLinks Module
"""

def joinConcat(df1, df2, key, cols):
  df1 = df1.sort_values(key)
  df2 = df2.sort_values(key)
  df = df1.merge(df2.drop_duplicates(), on=key, how='outer', indicator=True)
  df = df[cols] 
  df = df.drop_duplicates(keep="first")  
  return df

def joinLeftOnly(df1, df2, key, cols):
  df1 = df1.sort_values(key)
  df2 = df2.sort_values(key)
  df = df1.merge(df2.drop_duplicates(), on=key, how='left', indicator=True)
  df = df[df['_merge'] == 'left_only']
  df = df[cols] 
  df = df.drop_duplicates(keep="first")  
  return df

def joinRightOnly(df1, df2, key, cols):
  df1 = df1.sort_values(key)
  df2 = df2.sort_values(key)
  df = df1.merge(df2.drop_duplicates(), on=key, how='right', indicator=True)
  df = df[df['_merge'] == 'right_only']
  df = df[cols] 
  df = df.drop_duplicates(keep="first")  
  return df

import pandas as pd
df1 = pd.DataFrame(data = {'col1' : [2, 1, 3, 4, 5, 3], 
                           'col2' : [10, 11, 12, 13, 14, 10]}) 
df2 = pd.DataFrame(data = {'col1' : [5, 2, 2, 8],
                           'col2' : [10, 11, 2, 12]})
cols = ["col1"]
joinCol = 'col1'
# https://stackoverflow.com/questions/28901683/pandas-get-rows-which-are-not-in-other-dataframe
df_left = joinConcat(df1, df2, joinCol, cols)
  
# df_left = pd.concat([df1, df2], axis=0, join='outer', ignore_index=True, keys=None,
#           levels=None, names=None, verify_integrity=False, copy=True)
# df_left = df_left[["col1"]]   
# df_left = df_left.drop_duplicates(keep="first")          

# df_right = df1.merge(df2.drop_duplicates(), on=['col1'], 
#                    how='right', indicator=True)
# df_right = df_right[df_right['_merge'] == 'right_only']
df_right = joinLeftOnly(df1, df2, joinCol, cols)

print(df_left)


print(df_right)