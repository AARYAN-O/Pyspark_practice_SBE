from pyspark.sql.types import StringType,StructType,StructField,IntegerType

schema=StructType([StructField('Person',StringType(),True),StructField('Roll_No',IntegerType(),True)])

data=[('A',1),('B',2)]

df=spark.createDataFrame(data,schema)

print(df.count())
