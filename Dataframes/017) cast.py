# in the following example , we are typcasting the value of one column from Integer to String

data=[('Peter',12),('Parker',14)]
from pyspark.sql.types import StructType , StructField , StringType,IntegerType
from pyspark.sql.functions import col

schema=StructType([
    StructField('firstname',StringType(),True),
    StructField('Age',IntegerType(),True)]
)

df=spark.createDataFrame(data,schema)

# Note ,in the cast function , we need to use values like String and not StringType
df.withColumn('Age',col('Age').cast('String')).printSchema()

df.printSchema()
