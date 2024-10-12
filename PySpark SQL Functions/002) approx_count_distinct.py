# What was the use of approx_count_distinct when we were already having count() and distinct() ?

# The reason is that when we have a very large dataset , it is used for optimization purposes.

from pyspark.sql.functions import approx_count_distinct

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")
df1=df.select(approx_count_distinct("movieId"))
df1.display()
