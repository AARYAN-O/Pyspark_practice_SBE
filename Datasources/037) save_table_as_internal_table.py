df=spark.read.format("json").option("header","true").option("multiline","true").load("dbfs:/FileStore/people.json")

df.write.mode("overwrite").saveAsTable("temp_table")

df.display()

df1=spark.read.table("temp_table")

df1.display()

# What are internal tables?

# Those tables where control over metadata information as well as actual information is stored on the disk.
# Internal tables are also called as managed tables.
# Here, dropping tables remove both metadata and the data 
# The data here is stored inside spark's warehouse directory.

# What are external tables ?

# These are tables pointing to data stored externally (in external systems like hdfs, s3, azure blob, etc)
