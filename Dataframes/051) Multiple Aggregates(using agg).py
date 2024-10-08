# dbfs:/FileStore/superstore.csv
# dbfs is databricks file system means that the data is stored inside databricks

from pyspark.sql.functions import col,sum

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")
df.display()
df=df.withColumn("Profit",col("Profit").cast("Integer"))
df.printSchema()
df=df.groupBy("Country").agg(sum("Profit"))
df.display()

# Whenever you would want to use aggregations using agg() , i.e. whenever you would want to use :
# sum,mean,avg,count,min & max , you should import them from pyspark.sql.functions because if you dont 
# python will get confused if these are pyspark's functions or python's functions.

# Multiple Aggregation Example

from pyspark.sql.functions import col,count

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")
df.printSchema()
df1=df.groupBy("City").agg(count("ShipMode"),sum("Discount"))
df1.display()
