# Note : Use collect only when we have smaller amount of data.
from pyspark.sql.types import StructType, StructField, StringType
data=[("Peter",1),("Parker",2)]
col_names=['name','roll']

df=spark.createDataFrame(data,col_names)

df.show()

# Note : Usually, collect() is used to retrieve the action output when you have very small result set and calling 
# collect() on an RDD/DataFrame with a bigger result set causes out of memory as it returns the entire dataset 
# (from all workers) to the driver hence we should avoid calling collect() on a larger dataset.

df.collect()

# Out[5]: [Row(name='Peter', roll=1), Row(name='Parker', roll=2)]

# As you can see in the above function that collect() returns an array.

# How to access various elements of the array ?

# Since the arrays are indexable , we can point out to specific values as well.

df.collect()[0]
# Row(name='Peter', roll=1)
# the above statement returns the first row.

df.collect()[0][0]
# Out[6]: 'Peter'
# the above statement returns the value at first row and first column.


# Can we assign collect to some variables and print each row separately ?
# Yes

collection=df.collect()
# collection.show() --This won't work because collect returns a list and not a dataframe . So show is valid for a dataframe 
# but not a list 

print(collection)

# or 

for value in collection:
  print(value)


# collect with select 
# we can use select before collect to filter out all the columns that are needed.

collection=df.select('name').collect()
print(collection)
# [Row(name='Peter'), Row(name='Parker')]
