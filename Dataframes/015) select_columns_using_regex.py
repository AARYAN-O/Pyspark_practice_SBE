#Note that since spark is written in scala , it relies on the scala regex (which is very similiar to java)

data=[('Peter',20),('Tony',40)]

column_names=['Name','Age']

rdd=spark.sparkContext.parallelize(data)

df=rdd.toDF(schema)

df.select(df.colRegex("`^.*name*`")).show()


