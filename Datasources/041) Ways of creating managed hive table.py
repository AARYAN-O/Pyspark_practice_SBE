# /user/hive/warehouse -- location of the file when we use the first method

# Using spark sql

spark.sql("create table my_managed_table(id int,name string) stored as parquet")

# Using dataframe API

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.write.mode("overwrite").format("orc").saveAsTable("my_managed_table")

spark.sql("describe extended my_managed_table").display()

# Note : When we do paritionBy , we will get everything vconverted to the columnar format
