df=spark.read.format("json").option("multiline","true").load(["dbfs:/FileStore/people.json","dbfs:/FileStore/test.json"])
df.display()
