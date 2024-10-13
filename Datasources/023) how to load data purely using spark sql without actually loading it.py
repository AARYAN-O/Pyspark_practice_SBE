spark.sql("create or replace temporary view new_temp using parquet options (path 'dbfs:/FileStore/users.parquet')")

spark.sql("select * from new_temp").display()
