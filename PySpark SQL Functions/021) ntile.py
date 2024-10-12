# If the value of n in ntile is 4 , then it becomes a quartile.

from pyspark.sql.window import Window
from pyspark.sql.functions import ntile

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/ratings.csv")

window_spec=Window.partitionBy("rating").orderBy("movieId")

df=df.withColumn("ntile_value",ntile(4).over(window_spec))

df.display()
