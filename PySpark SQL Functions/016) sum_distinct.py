# sum_distinct gives the sum of the distinct values inside the column only

from pyspark.sql.functions import sum_distinct

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(sum_distinct("movieID")).display()
