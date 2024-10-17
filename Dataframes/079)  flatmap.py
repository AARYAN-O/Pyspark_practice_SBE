data=[1,2,3,4]
rdd1=spark.sparkContext.parallelize(data)

rdd2=rdd1.flatMap(lambda x:(x**1,x**2))

rdd2.collect()

# flatmap wont work on dataframes , they only work on RDDs
# In order to make something similiar to flatmap, we need to use explode