from pyspark.sql import SparkSession

spark=SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("create or replace table employees(id INT , name string)")
spark.sql("insert into table employees values (1,'John Doe')")

df=spark.sql("select * from employees")
df.display()

# How to check for hive tables ?
# spark.sql('SHOW TABLES').display()

# hive tables are temporary and only remain until the session ends.

# database	tableName	isTemporary
# default	employees	false
# 	temp_view	true
