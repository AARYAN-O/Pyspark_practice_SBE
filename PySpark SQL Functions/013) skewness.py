from pyspark.sql.functions import skewness

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(skewness("movieId")).display()

# here we are getting a skewness of 1.1272958395618713

# Positive skewness means that there are more extreme values on the higher end(right side) of the distribution
# Negative skewness means that there are more extreme values on the lowwer end (left side) of distribution
