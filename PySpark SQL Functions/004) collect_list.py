from pyspark.sql.functions import collect_list

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(collect_list("movieId")).display()

# collect_list returns list of all values 
