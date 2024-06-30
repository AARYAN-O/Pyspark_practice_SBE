from pyspark.sql.types import StringType, StructType , StructField
# when we are filling the data , fill it inside the RDD first 
# In order to fill the data inside the RDD , we need to write tuples (representation of rows ) within lists.
# We don't even need to give the schema to tell the interpreter about the data.
# Rather , we can choose just to give the data.

data=[('Peter','Parker',30),('Tony','Stark',40)]

df=spark.createDataFrame(data)

df.printSchema()

df.show()
