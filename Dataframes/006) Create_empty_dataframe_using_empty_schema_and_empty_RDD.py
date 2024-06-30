emptyRDD=spark.sparkContext.parallelize([])

from pyspark.sql.types import StructType
schema=StructType([])

df1=spark.createDataFrame(emptyRDD,schema)
df1.printSchema()
