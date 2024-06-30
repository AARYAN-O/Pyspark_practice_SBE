# creating pyspark dataframe
data=[('Peter','Parker',30),('Tony','Stark',40)]
rdd=spark.sparkContext.parallelize(data)

column_names=['Name','Last Name','Age']

# toDF contains only the names of the columns
df=rdd.toDF(column_names)

# createDataFrame will consist of columnnames as well as schema 
# df=spark.createDataFrame(data,schema)

pandas_df=df.toPandas()

pandas_df.head()
