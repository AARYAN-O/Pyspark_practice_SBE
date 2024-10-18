data=[("A",1),("B",2)]
rdd=spark.sparkContext.parallelize(data)

print(rdd.collect())

print(rdd.sample(False,0.5).collect())
