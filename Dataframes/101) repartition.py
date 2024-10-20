# When can reparition not happen properly?
# When the data is very highly skewed , repartition may not happen properly

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")

df.display()

df.write.partitionBy("region").mode("overwrite").csv("dbfs:/output/test_csv123.csv")

pdf=spark.read.format("csv").option("header","true").load("dbfs:/output/test_csv123.csv")

print(df.rdd.getNumPartitions())

df=df.repartition(2)

print(df.rdd.getNumPartitions())

# Note : We need to collect the repartitioned dataframe as well
