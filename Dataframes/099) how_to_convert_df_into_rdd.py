# The default number of parititions in repartition is 200.

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/bank_customer.csv")

rdd1=df.rdd

print(rdd1.collect())
