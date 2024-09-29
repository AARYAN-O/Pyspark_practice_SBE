from pyspark.sql.types import StructType,StructField,IntegerType,StringType

schema=StructType([StructField('Person',StringType(),True),StructField('City',StringType(),True)])
data=[('Person2','City1'),('Person1','City2')]

df=spark.createDataFrame(data,schema)

df1=df.sort('Person')
df1.show()


