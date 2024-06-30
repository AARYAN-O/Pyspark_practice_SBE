data=[('Peter','Parker',12),('Tony','Stark',20)]

column_names=['Firstname','Lastname','Age']

rdd=spark.sparkContext.parallelize(data)

# Following are two methods we can initialize dataframes 

# 1)  df=rdd.toDF(column_names)

# OR

# 2) df=spark.createDataFrame(data,schema=column_names)


df=spark.createDataFrame(data,schema=column_names)

# By default, it shows only 20 Rows, and the column values are truncated at 20 characters.
df.show()

# only show 1 row
df.show(1)

# by default , if the number of characeters exceed in the row in df , then some characters are truncated a
# and the truncated characters are shown by elipsis (...)
# by default , the truncate is set to True
df.show(1,truncate=False)

# we can even set the number of characters 
df.show(1,truncate=2)

# vertical will show the columns as rows and rows as columns 
df.show(1,truncate=2,vertical=True)

# using n
df.show(n=1)

