from pyspark.sql.functions import max

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(max("movieId")).display()
