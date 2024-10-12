# it returns the first existing value in the data
# If the first value is Null , then it will return null as well.

from pyspark.sql.functions import first

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(first('movieId')).display()

# When we set ignoreNulls=True , we will get the return value as first non-null value 

df.select(first('movieId',ignorenulls=True)).display()

