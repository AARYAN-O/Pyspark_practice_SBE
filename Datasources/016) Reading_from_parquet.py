df=spark.read.parquet("dbfs:/FileStore/users.parquet")

df.display()


# or

df=spark.read.format("parquet").load("dbfs:/FileStore/users.parquet")

df.display()
