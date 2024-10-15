spark.sql("create database db2")

df.write.mode("overwrite").saveAsTable("db2.temp_table")

spark.read.table("temp_table").display()


# What if we want to store the data inside a particular database within hive.
# /user/hive/warehouse/db1.db/temp_table

# Note : using spark.sql() , the data gets stored by default in the parquet format.
# parquet is columnar storage file and is used for data processing usually

