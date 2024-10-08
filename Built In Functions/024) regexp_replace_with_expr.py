from pyspark.sql.functions import expr 
 
df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df=df.withColumn("new_column",expr("regexp_replace('movieId','title','genres')"))

df.display()
