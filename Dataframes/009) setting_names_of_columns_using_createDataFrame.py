data=[('Peter','Parker',30),('Tony','Stark',40)]
rdd=spark.sparkContext.parallelize(data)

# We can use the parameter called schema present inside createDataFrame to assign it a list containing the names of the columns

column_names=['firstname','lastname','age']

df=spark.createDataFrame(rdd,schema=column_names)

df.show()
