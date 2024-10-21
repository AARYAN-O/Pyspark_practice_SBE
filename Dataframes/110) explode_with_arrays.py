from pyspark.sql.types import StringType,StructField,ArrayType
from pyspark.sql.functions import explode

schema=StructType([StructField("name",StringType(),True),StructField("colors",ArrayType(StringType()),True)])
data=[("A",["red","blue"]),("B",["blue","yellow"])]

df=spark.createDataFrame(data,schema)

df.select("name",explode(df.colors)).display()
