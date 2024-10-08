from pyspark.sql.functions import concat_ws

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.createOrReplaceTempView("new_df")

spark.sql("select movieId,concat_ws(',',title,genres) as concatenated_column from new_df").display()
