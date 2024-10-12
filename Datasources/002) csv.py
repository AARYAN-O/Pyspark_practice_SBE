# Note : When you want to upload the file , we need to go to dbfs and click on upload and then browse.

df=spark.read.option("header","true").csv("dbfs:/FileStore/world_bank_latest.csv")
df.display()
