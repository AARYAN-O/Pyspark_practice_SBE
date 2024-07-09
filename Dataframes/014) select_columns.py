data=[('Peter','Parker',12),('Tony','Stark',14)]

column_names=['firstname','lastname','Age']
df=spark.createDataFrame(data,column_names)

# 1)
df.select('firstname','Age').show()

# 2) 
df.select(df.firstname,df.Age).show()

# 3) 
df.select(df['firstname'],df['Age']).show()

# 4)  using col

from pyspark.sql.functions import col
df.select(col('firstname'),col('lastname')).show()
