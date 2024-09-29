# For uploading the files,
# click on catalog => dbfs => Upload the file inside the filestore => click on the down arrow => and then copy path
# Note: spark is a keyword in pyspark

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/bank_customer.csv")
df.show()
df.createOrReplaceTempView("EMP")
spark.sql("select * from EMP").show()
spark.sql("select cust_id,cust_fname from EMP").show()
