# Note : Use collect only when we have smaller amount of data.
from pyspark.sql.types import StructType, StructField, StringType
data=[("Peter",1),("Parker",2)]
col_names=['name','roll']

df=spark.createDataFrame(data,col_names)

df.show()

# Note : collect() should only be applied in case we have smaller data.

df.collect()

# Out[5]: [Row(name='Peter', roll=1), Row(name='Parker', roll=2)]

# As you can see in the above function that collect() returns an array.

# How to access various elements of the array ?

# Since the arrays are indexable , we can point out to specific values as well.

df.collect()[0]
# the above statement returns the first row.

df.collect()[0][0]
# the above statement returns the value at first row and first column.
