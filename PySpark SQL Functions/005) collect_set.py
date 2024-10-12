from pyspark.sql.functions import collect_set

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(collect_set("movieId")).display()

# Note : Since , the values inside the set are unordered , we will be having an unordered set.
