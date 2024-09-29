from pyspark.sql.functions import col

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")
df.printSchema()
df.groupBy("Country").count().display()
