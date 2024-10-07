# pyspark.sql.functions.split(str, pattern, limit=-1)

from pyspark.sql.functions import split,col

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df=df.withColumn("genre array",split(col('genres'),"\|"))

df.display()

# Note that whenever , there are problems created using the pipe operator , it can be because of the fact that pipe operator
# is being considered as OR operator and to prevent that, we need to use the backslash (\)
