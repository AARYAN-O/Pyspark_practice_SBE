from pyspark.sql.functions import col

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")

df.display()
df=df.withColumn("Profit",col("Profit").cast("Integer"))

df.groupBy("Country").mean().display()

df.groupBy("Country").avg().display()
