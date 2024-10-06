from pyspark.sql.functions import expr

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df1=df.withColumn("concatenated column",expr("movieId ||' '|| title"))

df1.display()
