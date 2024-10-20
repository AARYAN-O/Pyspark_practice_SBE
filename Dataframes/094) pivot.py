from pyspark.sql.functions import max,col
from pyspark.sql.types import StructType,StructField,IntegerType

# pivot_columns: The column whose distinct values become new columns.

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")

df.display()

df.printSchema()

df=df.withColumn("discount",col("discount").cast("Integer"))

pivot_df=df.groupBy("ShipMode").pivot("Segment").max("discount")

pivot_df.display()
