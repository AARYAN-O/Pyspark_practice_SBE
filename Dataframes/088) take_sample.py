data=[1,2,3,4]
rdd=spark.sparkContext.parallelize(data)

print(rdd.takeSample(False,2,124))
