# For uploading the files,
# click on catalog => dbfs => Upload the file inside the filestore => click on the down arrow => and then copy path

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/bank_customer.csv")
df.show()
