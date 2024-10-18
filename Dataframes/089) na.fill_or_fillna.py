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

df=df.fillna(value='unknown',subset=['state-province'])

# Note that the subset can be a list. Hence we can apply that logic to multiple columns.
# Even if you remove the list , everything will work fine 

df=df.fillna(value='unknown',subset='state-province')

print("new dataframe")

df.display()



# *****************************



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

df=df.na.fill(value='unknown',subset='state-province')

print("new dataframe")

df.display()

