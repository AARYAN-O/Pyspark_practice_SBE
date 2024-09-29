from pyspark.sql.types import StringType,IntegerType,StructType,StructField
from pyspark.sql.functions import col

schema=StructType([StructField('Person',StringType(),True),StructField('Roll_No',IntegerType(),True)])

data=[('Person2',12),('Person1',14)]

df=spark.createDataFrame(data,schema)

df1=df.sort(col('Person'))

df1.show()
