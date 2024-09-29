from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import col

schema=StructType([StructField('Person',StringType(),True),StructField('Roll_No',IntegerType(),True),
StructField('City',StringType(),True)])

data=[('Person1',12,'City1'),('Person2',14,'City2')]

df=spark.createDataFrame(data,schema)

df1=df.orderBy(col('City').asc(),col('Person').desc())

df1.show()

# Note : When we have multiple sorting conditions , then sorting happens according to the first condition and then 
# only when the first condition is tied, it moves to the second condition 
