# array contains returns a boolean value of whether an array contains a particular value or not.

from pyspark.sql.functions import array_contains 
from pyspark.sql.types import ArrayType,IntegerType

schema=StructType([StructField("Name",StringType(),True),StructField("colors",ArrayType(StringType()),True)])

data=[("A",["red","blue"]),("B",["green","orange"])]

df=spark.createDataFrame(data,schema)

df.display()

df=df.withColumn("new colors",array_contains(df.colors,"red"))

df.display()
