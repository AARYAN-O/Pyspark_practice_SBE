# Here rank is calculated based on percentage terms from 0 to 1.
# 0 represents 0 percentage and 1 represents 100 percentage

from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/ratings.csv")

window_spec=Window.partitionBy("rating").orderBy("movieId")

df=df.withColumn("percent rank",percent_rank().over(window_spec))

df.display()
