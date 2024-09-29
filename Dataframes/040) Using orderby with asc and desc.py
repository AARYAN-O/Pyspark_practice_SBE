from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import col

schema=StructType([StructField('Person',StringType(),True),StructField('Roll_No',IntegerType(),True)])
data=[('Person2',12),('Person1',14)]

df=spark.createDataFrame(data,schema)

df1=df.orderBy(col('Person').asc())

df2=df.orderBy(col('Person').desc())

df1.show()
df2.show()
