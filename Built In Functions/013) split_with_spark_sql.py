df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")
df.createOrReplaceTempView("my_db")

# We need to use 4 backslashes when we are using sql instead of using 1 backslash that we normally use in python
spark.sql("select genres,split(genres,'\\\\|') as genre_array from my_db").display()
