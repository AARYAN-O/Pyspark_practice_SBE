from pyspark.sql.window import Window
from pyspark.sql.functions import rank 

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/ratings.csv")

window_spec=Window.partitionBy("rating").orderBy("movieId")

df=df.withColumn("rank",rank().over(window_spec))

df.display()
