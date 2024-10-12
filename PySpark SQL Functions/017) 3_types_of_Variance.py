from pyspark.sql.functions import variance,var_pop,var_samp

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(variance("movieId")).display()

df.select(var_pop("movieId")).display()

df.select(var_samp("movieID")).display()
