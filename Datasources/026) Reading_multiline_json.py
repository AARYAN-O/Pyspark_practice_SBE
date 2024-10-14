df=spark.read.option("header","true").option("multiline","true").json("dbfs:/FileStore/test.json")

df.display()

# If we do not set multiline=True , then we will get an error in case json is multiline
