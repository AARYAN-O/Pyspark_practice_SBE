df=spark.read.format("parquet").load("dbfs:/FileStore/users.parquet")

df.display()

df.write.mode("append").parquet("dbfs:/output/sales_us/new_users.parquet")

df=spark.read.format("parquet").load("dbfs:/output/sales_us/new_users.parquet")

df.display()

# if the file exists , it will append the later data into the former data inside the file,
# If the file does not exist , then it will create a new file.
