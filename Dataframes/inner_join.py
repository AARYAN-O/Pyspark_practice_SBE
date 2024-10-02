df1=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")
df2=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/ratings.csv")

# df1.display()
# df2.display()

df1.join(df2,df1.movieId==df2.movieId,"inner").display()
