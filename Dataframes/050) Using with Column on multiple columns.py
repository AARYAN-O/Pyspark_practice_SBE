from pyspark.sql.types import IntegerType,StructType
from pyspark.sql.functions import col

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")
df.display()
df.printSchema()
df=df.withColumn("Quantity",col("Quantity").cast("Integer")).withColumn("Discount",col("Discount").cast("Integer"))
df.printSchema()
df=df.groupBy("Country","State").sum("Quantity","Discount")
df.display()
