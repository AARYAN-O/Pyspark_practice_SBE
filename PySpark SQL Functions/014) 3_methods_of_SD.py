from pyspark.sql.functions import stddev,stddev_pop,stddev_samp

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(stddev("movieid"),stddev_samp("movieId"),stddev_pop("movieId")).display()
