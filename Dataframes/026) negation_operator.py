# using <> / ~ operator 

# we can use negation operator instead of not equal to operator as well

from pyspark.sql.functions import col

data=[('Peter',12),('Tony',12),('Falcon',16)]
column_names=['Name','Age']

df=spark.createDataFrame(data,column_names)

df1=df.filter(col('Age')!=12)

df2=df.filter(~(col('Age')==12))

df1.show()

df2.show()
