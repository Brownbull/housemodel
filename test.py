# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  scrapLinks Module
"""
import pandas as pd
df1 = pd.DataFrame(data = {'col1' : [2, 1, 3, 4, 5, 3], 
                           'col2' : [10, 11, 12, 13, 14, 10]}) 
df2 = pd.DataFrame(data = {'col1' : [5, 2, 8],
                           'col2' : [10, 11, 12]})
# https://stackoverflow.com/questions/28901683/pandas-get-rows-which-are-not-in-other-dataframe
df_left = df1.merge(df2.drop_duplicates(), on=['col1'], 
                   how='left', indicator=True)
df_left['_merge'] == 'left_only'
df_left = df_left[df_left['_merge'] == 'left_only']

df_right = df1.merge(df2.drop_duplicates(), on=['col1'], 
                   how='right', indicator=True)
df_right = df_right[df_right['_merge'] == 'right_only']

print(df_left)


print(df_right)