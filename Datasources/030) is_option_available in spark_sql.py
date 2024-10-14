spark.sql("create or replace temporary view temp_view using json options (path 'dbfs:/FileStore/*.json' , multiline 'true')")

spark.sql("select * from temp_view").display()
