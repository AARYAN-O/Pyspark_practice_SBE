df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")
df.display()

# display helps you see the data in a tabular format
