# /user/hive/warehouse -- location of the file when we use the first method

# Using spark sql

spark.sql("create table my_managed_table(id int,name string) stored as parquet")

# Using dataframe API

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.write.mode("overwrite").format("orc").saveAsTable("my_managed_table")

spark.sql("describe extended my_managed_table").display()

# Note : When we do paritionBy , we will get everything vconverted to the columnar format

# Note that when we use "stored as" , we need to mention the schema as well.

# create or replace is not present in pyspark.

# with or without "stored as" , we will be getting the table stored as managed table.


