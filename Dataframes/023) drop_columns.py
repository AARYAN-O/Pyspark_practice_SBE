from pyspark.sql.types import StringType, StructType , IntegerType

# Note that parallelize can contain any type of iterable... list , tuple,etc
# But when we want to data to have tuples inside of lists or collection inside a collection , that thing may not work.
# data=[('Peter',12),('Tony',14)]
# rdd=spark.sparkContext.parallelize(data)

# df=rdd.toDF(data)

# df.show()

data=[('Peter',12),('Tony',14)]
column_names=['Name','Age']

df=spark.createDataFrame(data,column_names)
df.show()

# pyspark allows us to drop the columns as well.
df1=df.drop('Age')
df1.show()

# dropping multiple columns
df2=df.drop('Age','Name')
df2.show()
# does order of drop matters ? --> NO you could have even written drop('Name','Age') there and it still would have worked.
