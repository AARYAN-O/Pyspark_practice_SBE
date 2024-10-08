# substring with select 

from pyspark.sql.functions import substring

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select('genres',substring('genres',2,10).alias("substringed genres"),substring('title',2,10).alias("substringed titles")).display()
