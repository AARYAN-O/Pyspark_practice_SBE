# Note that we can use withColumn(existing_column, transformations on the existing columns) to overwrite values over the 
# existing columns

from pyspark.sql.functions import expr

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df2=df.withColumn("movieId",expr("case when movieId%2==0 then 'odd' when movieId%2<>0 then 'even' else 'nothing' end "))

df2.display()
