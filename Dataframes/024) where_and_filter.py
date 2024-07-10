from pyspark.sql.types import StringType , StructType , IntegerType
from pyspark.sql.functions import col

data=[('Tony',12),('Peter',14)]

column_names=['first_name','last_name']

# here in the second parameter , we could have put either of column_names or schema(the one made with the help of 
# StructType , StructField ,etc) . If we do not put schema , then also everything works fine , it would just 
# interpret schema by itself.

df=spark.createDataFrame(data,column_names)

# What is the difference between where and filter in PySpark?
# In PySpark, both filter() and where() functions are used to select out data based on certain conditions. 
# They are used interchangeably, and both of them essentially perform the same operation.

df1=df.where(col('first_name')=='Tony')
df1.show()

df2=df.filter(col('first_name')=='Tony')
df2.show()

# Note that df.select returns a new dataframe 
# for us to do the chaining of functions , we need to ensure that the functions return the new dataframe , if the previous function does not return
# a new dataframe , it wont work

df3=df.select('first_name').where(col('first_name')=='Tony')
df3.show()
