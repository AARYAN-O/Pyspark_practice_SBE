from pyspark.sql.types import StructType, StructField , StringType, IntegerType

# this will return the dataframe after dropping the duplicates

schema=StructType([StructField('Person',StringType(),True),StructField('Roll_No',IntegerType(),True)])

data=[('A',12),('B',16),('A',12),('D',16)]

df=spark.createDataFrame(data,schema)

df.show()

df1=df.dropDuplicates()

df1.show()
