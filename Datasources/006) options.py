df=spark.read.options(delimiter=",",inferSchema="True",header="True").csv("dbfs:/FileStore/movies.csv")

df.display()
