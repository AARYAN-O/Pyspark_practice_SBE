from pyspark.sql.functions import lit,expr,col

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df1=df.withColumn("lit_value",when(col("movieId")%2==0,lit("ok")).otherwise("not ok"))

df1.display()

# Using lit function with expr is a bit difficult
