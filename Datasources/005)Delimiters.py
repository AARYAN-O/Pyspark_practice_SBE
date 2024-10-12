# By default , the delimiter is comma ; but you can set it to anything else as well.
# Note that we need to use options instead of option here.

# option (): This function can support only single attribute/operation but multiple option () function can be used in
# series. options (): This function can support multiple attributes/operations using comma separated Key value pairs.

df=spark.read.options(delimiter=",").csv("dbfs:/FileStore/movies.csv")
df.display()
