from pyspark.sql.window import Window 
from pyspark.sql.functions import dense_rank

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/ratings.csv")

window_spec=Window.partitionBy("rating").orderBy("movieId")

df=df.withColumn("dense_rank",dense_rank().over(window_spec))

df.display()

# https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEiI-mTDNOHmxIAsyeLA45ctIwYpc8sat7cmcXqbed7u56dYwbqJQGn3suzDeK3POUIogb2ohhWDk_IRGuoyQxO6na_UkKxFosa598Rt1uUTrzKPWT8yiVr_iiLAP6YXDY8kLzCKK0arVwo/s1600/Slide4.PNG

# dense_rank ==> ranking system in schools with ties 
# row_number==> ranking system in school without ties
# rank ==> wierd
