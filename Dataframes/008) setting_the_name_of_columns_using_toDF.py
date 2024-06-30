data=[('Peter','Parker',30),('Tony','Stark',40)]
rdd=spark.sparkContext.parallelize(data)

from pyspark.sql.types import StructType, StringType,StructField

# toDF is where you can set the names of the column.
# pass a list of strings (which correspond to the column names here)
df=rdd.toDF(['Name','Last Name','Age'])

df.show()
