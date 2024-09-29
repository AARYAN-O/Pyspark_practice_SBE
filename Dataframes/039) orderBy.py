from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import col

schema=StructType([StructField('Person',StringType(),True),StructField('City',StringType(),True)])

data=[('Person1','City1'),('Person2','City2')]

df=spark.createDataFrame(data,schema)

df.orderBy('Person')

df.show()
