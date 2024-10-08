from pyspark.sql.functions import col

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.display()

df=df.withColumn("new_title",col("title").substr(2,10))

df.display()
