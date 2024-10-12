# cume_dist represents cumulative distribution
# The graph of cumulative distribution will always be increasing 

from pyspark.sql.window import Window
from pyspark.sql.functions import cume_dist

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/ratings.csv")

window_spec=Window.partitionBy("rating").orderBy("movieId")

df=df.withColumn("cume_dist",cume_dist().over(window_spec))

df.display()
