# In pyspark, if we have multiple columns which needs sorting , we can even sort each of those columns differently -
# some of them can be sorted by ascending order and some can be sorted by descending order

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

schema=StructType([StructField('Person',StringType(),True),StructField('City',StringType(),True),StructField('State',StringType(),True)])
data=[('Person2','City1','State1'),('Person1','City2','State2')]

df=spark.createDataFrame(data,schema)

df1=df.sort('Person','State',ascending=[True,False])
df1.show()
