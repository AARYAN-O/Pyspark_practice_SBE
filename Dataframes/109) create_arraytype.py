from pyspark.sql.types import ArrayType,StructField,StructType,StringType,IntegerType

schema=StructType([StructField("name",StringType(),True),StructField("numbers",ArrayType(IntegerType()),True)])
data=[("A",[10,12]),("B",[14,16])]

df=spark.createDataFrame(data,schema)

df.display()

# Note : ArrayType(x) where x is the datatype
