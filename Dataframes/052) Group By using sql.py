df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")
df.createOrReplaceTempView("SuperStore")
spark.sql("select Country,State,min(Profit),max(Profit) from SuperStore group by Country,State").display()

# Note : In sql statements, we dont need to mention column names with double or single quotes.
# Write column names directly.
