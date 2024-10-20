df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")

df.display()

df.write.partitionBy("category").mode("overwrite").csv("dbfs:/output/test_csv123.csv")

df=spark.read.format("csv").option("header","true").load("dbfs:/output/test_csv123.csv")

df.display()

print(df.rdd.getNumPartitions())

df=df.coalesce(4)

print(df.rdd.getNumPartitions())
