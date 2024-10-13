df=spark.read.format("parquet").load("dbfs:/FileStore/users.parquet")

df.display()

df.write.mode("ignore").parquet("dbfs:/output/sales_us/new_users.parquet")

df=spark.read.format("parquet").load("dbfs:/output/sales_us/new_users.parquet")

df.display()

# If anything exists in the writing path of the file , then that will be ignored if we use ignore in parquet.
