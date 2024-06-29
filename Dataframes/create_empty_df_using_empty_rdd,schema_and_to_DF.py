emptyRDD=spark.sparkContext.parallelize([])

from pyspark.sql.types import StringType,StructType,StructField

# Note that StructType must be outside everything and StructField must be inside that.
# Also note that inside StructType , we need to pass a list.

schema=StructType([
StructField('firstname',StringType(),True),
StructField('middlename',StringType(),True),
StructField('lastname',StringType(),True)]
)

df1=emptyRDD.toDF(schema)
df1.printSchema()
