from pyspark.sql.functions import min

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(min("movieId")).display()
