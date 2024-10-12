# If we have a csv file and there the value contains the comma inside it , if we normally put that value (containing comma) ,
# then it will be considered as a separate value.

# In that case , we can put them inside quotes

from pyspark.sql.functions import regexp_replace

df=spark.read.option("quote","\"").option("header","true").csv("dbfs:/FileStore/movies.csv")

df=df.withColumn("genres",regexp_replace("genres","\|",","))

df.display()
