from pyspark.sql.functions import avg

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(avg("movieID")).display()
