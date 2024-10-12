from pyspark.sql.window import Window

from pyspark.sql.functions import row_number

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/ratings.csv")

window_spec=Window.partitionBy("rating").orderBy("movieId")

df=df.withColumn("row_number",row_number().over(window_spec))

df.display()
