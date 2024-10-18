import requests
import pandas as pd

url='http://universities.hipolabs.com/search?country=United+States'
response=requests.get(url)

data=response.json()

df=pd.DataFrame(data)

df=spark.createDataFrame(df)

df.sample(0.1).display()

# sample with replacement 

df.sample(True,0.1).display()

# sample(withReplacement, fraction, seed=None) --> fraction does not gurantee exact number of fractions 

# What is the use of seed ? --> seed is used to generate the same samples. If we have same seed number , 
# it means that we will get random number.

df.sample(True,0.2,123) 

 # setting seed as 123

# Note that it doesn’t guarantee to provide the exact number of the fraction of records.

# A random seed (or seed state, or just seed) is a number (or vector) used to initialize a pseudorandom number generator
