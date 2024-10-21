from pyspark.sql.types import StructField,StructType,IntegerType,StringType
from pyspark.sql.functions import array

# array function should be in small letters only

schema=StructType([StructField("name",StringType(),True),StructField("color1",StringType(),True),StructField("color2",StringType(),True)])

data=[("A","red","green"),("B","blue","orange")]

df=spark.createDataFrame(data,schema)

df=df.withColumn("color_array",array(df.color1,df.color2))

df.display()
