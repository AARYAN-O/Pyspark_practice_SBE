data=[(('Peter','Parker'),12,'Buldhana'),(('Tony','Stark'),16,'Tadoba')]

rdd=spark.sparkContext.parallelize(data)
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

schema=StructType(
  [
  StructField('name',StructType([StructField('firstname',StringType(),True),StructField('lastname',StringType(),True)])),
  StructField('Age',IntegerType(),True),
  StructField('Location',StringType(),True)
              ]
  )

df=rdd.toDF(schema)

df.printSchema()

df.show()
