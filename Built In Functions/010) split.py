# split function also needs to be imported
# Note that all the built in functions that are used in pyspark seem to be imported only.

from pyspark.sql.functions import split,col

data=[('A,B,C',2000,'F',24000,),('D,E,F',4000,'M',200000000)]
schema=["name","date of year","gender","salary"]

df=spark.createDataFrame(data,schema)

df=df.withColumn("name_array",split(col('name'),","))

df.display()
