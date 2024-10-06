# Note that if we do not use option("header","true") , then the names of the columns will come as col1,col2...

from pyspark.sql.functions import expr

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df1=df.filter(expr("movieId%2==0")).display()

