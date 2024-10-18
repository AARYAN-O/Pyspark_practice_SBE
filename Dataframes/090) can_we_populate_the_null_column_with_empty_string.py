import requests
import json
import pandas as pd 

post_url='http://universities.hipolabs.com/search?country=United+States'

response=requests.get(post_url)


data=response.json()
# The data is a  list

df=pd.DataFrame(data)
# pd.DataFrame is a dataframe 

df=spark.createDataFrame(df)

df.display()

df=df.na.fill(value='',subset='state-province')

print("new dataframe")

df.display()
