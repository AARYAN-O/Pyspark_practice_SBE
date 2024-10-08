from pyspark.sql.functions import substring

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df=df.withColumn("substring column",substring(df.title,2,6))

df.display()
