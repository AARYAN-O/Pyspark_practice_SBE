from pyspark.sql.functions import sum

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(sum("movieId")).display()
