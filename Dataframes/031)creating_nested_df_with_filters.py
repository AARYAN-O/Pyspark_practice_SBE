from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import IntegerType,StringType,ArrayType

schema =StructType([StructField("Person",StringType(),True),StructField("Roll_No",IntegerType(),True),StructField("Marks_in_all_subjects",ArrayType(IntegerType()),True)])

print(schema)

data=[('Person1',12,[96,98]),('Person2',14,[100,94])]

df=spark.createDataFrame(data,schema)

print(df)

print(df.show())

print(df.Marks_in_all_subjects)

# print(df.filter(df.Marks_in_all_subjects==[96,98]))

# Note the line 18 will give us error because ,filter cannot be applied on the array types.

But filters can be applied on more scalar datatypes 

print(df.filter(df.Person=="Person1"))

