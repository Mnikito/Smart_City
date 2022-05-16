import pandas as pd
import csv
import numpy as np

#Read Dataset
df = pd.read_csv (r'C:\Users\Mike\Downloads\Micheal.csv')

#Changes time index to proper format
df['record_time'] = pd.to_datetime(df['record_time'])
df.columns =['number', 'unique_IDs', 'camera_ID', 'record_time']
#This parses through the dataset in 5 second increments. To change the time period, simply change the 5s
ddf = df.resample('5s', on='record_time').unique_IDs.nunique()
# More Info at https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.resample.html 

ddf.to_csv('C:/Users/Mike/Downloads/5_Seconds.csv', index=True)