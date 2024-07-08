# Note : Use collect only when we have smaller amount of data.
from pyspark.sql.types import StructType, StructField, StringType
data=[("Peter",1),("Parker",2)]
col_names=['name','roll']

df=spark.createDataFrame(data,col_names)

df.show()

# Note : collect() should only be applied in case we have smaller data.

df.collect()

# Out[5]: [Row(name='Peter', roll=1), Row(name='Parker', roll=2)]
