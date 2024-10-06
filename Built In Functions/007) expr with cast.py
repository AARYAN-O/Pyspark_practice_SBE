from pyspark.sql.functions import expr,cast

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df1=df.withColumn("movieId in strings",expr("cast(movieId as string)"))

df1.display()
