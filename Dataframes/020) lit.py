# lit() is imported from pyspark.sql.functions and stands for literal value.
# lit can populate values in a already existing column as well as in new columns depending on the first parameter
# that you put inside the withColumn()


# Note : withColumn is an transformation
# In pyspark , transformations return a new dataframe where as action doesn't

from pyspark.sql.types import StructType , StringType , IntegerType , StructField
from pyspark.sql.functions import col, lit

data=[('Peter',12),('Tony',14)]
schema=StructType(
  [StructField('Name',StringType(),True),
   StructField('Age',IntegerType(),True)
  ])

df=spark.createDataFrame(data, schema)

df.show()

df1=df.withColumn('Name',lit('Falcon34'))

df1.show()

df2=df.withColumn('Name2',lit('Falcon_new')

df2.show()
