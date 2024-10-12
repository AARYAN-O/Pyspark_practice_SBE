from pyspark.sql.functions import last
df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(last("movieId")).display()

# using ignorenulls=True

df.select(last("movieId",ignorenulls=True)).display()
