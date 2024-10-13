# For loading the data inside a temp view , you need to load the data inside the dataframe simply.

df=spark.read.format("csv").load("dbfs:/FileStore/users.parquet")

df.createOrReplaceTempView("new_table")

spark.sql("select * from new_table").display()
