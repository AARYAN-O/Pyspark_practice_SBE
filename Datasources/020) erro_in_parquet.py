df=spark.read.format("parquet").load("dbfs:/FileStore/users.parquet")

df.display()

df.write.mode("error").parquet("dbfs:/output/sales_us/new_users.parquet")

df=spark.read.format("parquet").load("dbfs:/output/sales_us/new_users.parquet")

df.display()

# error will exist if the file path already exists
