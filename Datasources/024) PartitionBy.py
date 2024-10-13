df=spark.read.option("header","true").csv("dbfs:/FileStore/superstore.csv")
df.display()

df.write.partitionBy("Country").mode("overwrite").csv("dbfs:/output/test_csv.csv")

df.display()

# The partitions will already get created in the catalog => dbfs =>filestore=> output file => you will find list of 
# countries because of parititions.
