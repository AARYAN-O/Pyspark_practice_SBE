from pyspark.sql.functions import col
df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")
df.display()
df.printSchema()
# Its better to always do printSchema in order to know what datatype are the columns into
df=df.withColumn("Sales",col("Sales").cast(IntegerType()))
df.groupBy("State").sum("Sales").show()
