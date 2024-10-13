df=spark.read.format("parquet").load("dbfs:/FileStore/users.parquet")

df.write.mode("overwrite").parquet("dbfs:/output/sales_us/new_users.parquet")

df1=spark.read.format("parquet").load("dbfs:/output/sales_us/new_users.parquet")

df1.display()
