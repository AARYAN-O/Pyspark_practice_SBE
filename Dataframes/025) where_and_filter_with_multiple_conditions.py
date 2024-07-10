from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import col

data=[('Peter',12,122),('Tony',16,124),('Falcon',16,124)]
col_names=['Name','Age','Roll']

df=spark.createDataFrame(data,col_names)

df.show()

df1=df.where((df.Age==16) & (df.Roll==124))

df1.show()
