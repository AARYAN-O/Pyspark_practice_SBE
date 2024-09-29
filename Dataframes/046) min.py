from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")
df=df.withColumn("Profit",col("Profit").cast("Integer"))

df.groupBy("Country").min().show()
