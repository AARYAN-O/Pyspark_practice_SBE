from pyspark.sql.functions import countDistinct

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")

df.display()

df=df.select(countDistinct("segment"),countDistinct("PostalCode"))

df.display()
