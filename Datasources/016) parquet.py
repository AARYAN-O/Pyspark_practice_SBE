df=spark.read.parquet("dbfs:/FileStore/users.parquet")

df.display()
