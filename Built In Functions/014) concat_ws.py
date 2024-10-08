from pyspark.sql.functions import concat_ws

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")
df=df.withColumn("concatenated column",concat_ws(" hehe ",df.title,df.genres))

df.display()
