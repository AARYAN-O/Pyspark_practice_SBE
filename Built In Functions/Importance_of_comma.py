from pyspark.sql.types import StringType,StructType,StructField

data=[('b',)]
# data=[("07 Dec 2021 04:35:05")]
schema=StructType([StructField("date",StringType(),True)])

df=spark.createDataFrame(data,schema)

df.display()

# Now if there is a single element , we need to put comma there because if we do not put comma , it will consider that
# it is not tuple , rather it is a set.
