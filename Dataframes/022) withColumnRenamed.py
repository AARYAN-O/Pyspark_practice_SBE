from pyspark.sql.types import StructType , StructField , IntegerType , StringType
from pyspark.sql.functions import col

data=[('Peter',12),('Ton',14)]

schema=StructType([
  StructField('Name',StringType(),True),
  StructField('Age',IntegerType(),True)
])

df=spark.createDataFrame(data,schema)

df1=df.withColumnRenamed('Name','Naam')

df1.show()

# What are the inputs in createDatFrame()?

# data also consists of  dataRDD or iterable
# an RDD of any kind of SQL data representation (Row, tuple, int, boolean, etc.), or list, pandas.DataFrame or numpy.ndarray.
