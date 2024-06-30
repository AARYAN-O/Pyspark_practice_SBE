data=[('Peter','Parker',30),('Tony','Stark',40)]

rdd=spark.sparkContext.parallelize(data)

from pyspark.sql.types import StringType,StructType,StructField,IntegerType

schema=StructType(
  [StructField('firstname',StringType(),True),
  StructField('lastname',StringType(),True),
  StructField('Age',IntegerType(),True)
  ]
)

df=spark.createDataFrame(rdd,schema)

df.printSchema()
df.show()
