data=[('Peter','Parker',30),('Tony','Stark',40)]
rdd=spark.sparkContext.parallelize(data)

from pyspark.sql.types import StructType, StringType,StructField

df=rdd.toDF(['Name','Last Name','Age'])

df.show()
