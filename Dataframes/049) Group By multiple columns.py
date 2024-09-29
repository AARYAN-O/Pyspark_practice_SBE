from pyspark.sql.functions import col

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")
df.printSchema()
df.display()
df=df.withColumn("Profit",col("Profit").cast("Integer"))
df=df.withColumn("ShoppingCost",col("ShippingCost").cast("Integer"))
df.groupBy("Country","City").sum("Profit","ShoppingCost").display()
