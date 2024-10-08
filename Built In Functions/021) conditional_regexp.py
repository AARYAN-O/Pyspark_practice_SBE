from pyspark.sql.functions import regexp_replace,col,when

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df=df.withColumn("genres",when(col('genres').endswith('sy'),regexp_replace(df.genres,'Adventure\|Animation\|Children\|Comedy\|Fantasy','okish')).otherwise('default'))

df.display()
