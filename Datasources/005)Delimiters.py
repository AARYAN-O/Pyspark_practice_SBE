# By default , the delimiter is comma ; but you can set it to anything else as well.
# Note that we need to use options instead of option here.

df=spark.read.options(delimiter=",").csv("dbfs:/FileStore/movies.csv")
df.display()
