# (* ParitionBy helps in doing parallel transformations *)

# The partitions will already get created in the catalog => dbfs =>filestore=> output file => you will find list of 
# countries because of parititions => hit any country and you will find state 

df=spark.read.option("header","true").csv("dbfs:/FileStore/superstore.csv")
df.display()

df.write.partitionBy("Country","State").mode("overwrite").csv("dbfs:/output/test_csv.csv")

df=spark.read.csv("dbfs:/output/test_csv.csv")

df.display()
