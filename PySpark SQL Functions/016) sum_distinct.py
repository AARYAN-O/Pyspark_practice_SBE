from pyspark.sql.functions import sum_distinct

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(sum_distinct("movieID")).display()
