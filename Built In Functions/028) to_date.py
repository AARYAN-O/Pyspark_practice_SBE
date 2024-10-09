from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import to_date

data=[("2024 08 01",)]
schema=["date"]

df=spark.createDataFrame(data,schema)

df.printSchema()

df=df.withColumn("new_column",to_date("date"))

df.printSchema()
