df=spark.read.format("json").option("multiline","true").option("header","true").load("dbfs:/FileStore/*.json")

df.display()

# loading files using pattern matching in address of file
