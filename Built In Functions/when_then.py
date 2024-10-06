from pyspark.sql.functions import when 

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.display()
df1=df.withColumn("new_column",when(df.genres=="Adventure|Animation|Children|Comedy|Fantasy","found adventure").otherwise("not found adventure"))

df1.display()
