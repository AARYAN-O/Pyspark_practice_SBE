import requests
import pandas as pd

url='http://universities.hipolabs.com/search?country=United+States'
response=requests.get(url)

data=response.json()

df=pd.DataFrame(data)

df=spark.createDataFrame(df)

df.display()
