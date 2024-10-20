df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/bank_customer.csv")

df.display()

df.write.partitionBy("cust_type").mode("overwrite").csv("dbfs:/output/test_csv.csv")

pdf=spark.read.format("csv").load("dbfs:/output/test_csv.csv")

pdf.display()

# when we use mode("overwrite"), it will overwrite not the old file with new file , rather it will check for the inner records
# in case the records match , then overriding will happen , otherwise overriding won't happen.
