# using <> / ~ operator 

# we can use negation operator instead of not equal to operator as well

from pyspark.sql.functions import col

data=[('Peter',12),('Tony',12),('Falcon',16)]
column_names=['Name','Age']

df=spark.createDataFrame(data,column_names)

df1=df.filter(col('Age')!=12)

df2=df.filter(~(col('Age')==12))

df1.show()

df2.show()

# -------------------------
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import IntegerType , StringType

schema=StructType([StructField("Person",StringType(),True),StructField("RollNo",StringType(),True)])

data=[("Person_1",12),("Person_2",14),("Person_1",16),("Person_2",18)]

df=spark.createDataFrame(data,schema)

df1=df.filter(df.Person!="Person_1")

df2=df.filter(~(df.Person=="Person_1"))

print(df1.show())

print(df2.show())

