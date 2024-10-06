from pyspark.sql.functions import when,col

# the problem statement here is that we need to create a new column named new_column which will have value as row number 14
# when the column title is Nixon (1995) and it will have the column title as Balto (1995) which will have the value as row number 12
# if any of these values is not found then just print the value as no problem

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df1=df.withColumn("new_column",when(col("title")=='Nixon (1995)',"row number 14").when(col("title")=="Balto (1995)","row number 12").otherwise("no problem"))

df1.display()
