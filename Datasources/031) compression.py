# The usual compression types are gzip , parquet ,etc 

# Note that the compression works with the write mode.

df=spark.read.format("json").option("header","true").option("multiline","true").option("compression","gzip").load("dbfs:/FileStore/*.json")

df.display()
