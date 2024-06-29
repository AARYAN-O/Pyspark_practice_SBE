emptyRDD=spark.sparkContext.emptyRDD()
print(emptyRDD)

# or

emptyRDD=spark.sparkContext.parallelize([])
print(emptyRDD)
