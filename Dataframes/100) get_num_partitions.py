df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/bank_customer.csv")

df.write.partitionBy("cust_type").mode("overwrite").csv("dbfs:/output/test_csv12.csv")

pdf=spark.read.format("csv").option("header","true").load("dbfs:/output/test_csv12.csv")

print(df.rdd.getNumPartitions())

pdf.display()

# Note : For getting the number of partitions using getnumparitions , we need to convert the dataframe into rdd.
# The rdd functions can even be applied on the dataframe by converting the dataframe into rdd.
# The advantage of pyspark over pandas comes in case of parallel processing only.
