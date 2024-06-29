emptyRDD=spark.sparkContext.parallelize([])
print(emptyRDD)

from pyspark.sql.types import StructType,StructField,StringType
# all the things related to struct are usually present inside the pyspark.sql
# StructType is used for creating schema.
# StructField are used for creating columns
# StructField consists of StructField(name, dataType, nullable)
schema=StructType([
    StructField('firstname',StringType(),True),
    StructField('middlename',StringType(),True),
    StructField('lastname',StringType(),True)
])

df=spark.createDataFrame(emptyRDD,schema)
df.printSchema()
