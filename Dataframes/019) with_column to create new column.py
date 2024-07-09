# withColumn can be used for creating a new column if we put the new columnname inside the first parameter of withColumn.
# withColumn can be used for updating a old column if we put the old columnname inside the first parameter of withColumn.

from pyspark.sql.types import StringType , StructType , IntegerType 
from pyspark.sql.functions import col

data=[('Peter',20),('Tony',40)]

schema=StructType(
  [StructField('Name',StringType(),True),
   StructField('Age',IntegerType(),True)
  ])

df=spark.createDataFrame(data,schema)

df1=df.withColumn('Age_Double',col('Age')*2)

df1.show()

