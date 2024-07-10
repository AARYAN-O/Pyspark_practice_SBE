from pyspark.sql.functions import col

# Tuples are used for representing rows in pyspark
data=[('Peter',40),('Toeny',60)]
column_names=['Name','Age']

df=spark.createDataFrame(data,column_names)

df1=df.where(col('Name').startswith('P'))

df1.show()

df2=df.where(col('Name').endswith('y'))

df2.show()

df3=df.where(col('Name').contains('e'))

df3.show()
