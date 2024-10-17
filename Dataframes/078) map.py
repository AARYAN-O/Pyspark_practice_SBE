data=[1,2,3,4]
rdd=spark.sparkContext.parallelize(data)

rdd2=rdd.map(lambda x:(x,1))

print(rdd2.collect())