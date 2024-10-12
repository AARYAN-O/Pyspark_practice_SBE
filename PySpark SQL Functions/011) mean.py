from pyspark.sql.functions import mean

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(mean("movieId")).display()
