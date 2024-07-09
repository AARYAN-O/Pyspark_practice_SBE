from pyspark.sql.types import StringType, StructType , IntegerType
from pyspark.sql.functions import col

data=[('Peter',20),('Tony',40)]

schema=StructType([
  StructField('FirstName',StringType(),True),
  StructField('Age',IntegerType(),True)
])

df=spark.createDataFrame(data,schema)

df.show()

df1=df.withColumn('Age',col('Age')*2)

df1.show()

