from pyspark.sql.functions import count

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(count("*")).display()

# Note :Make sure that you are importing the aggregate functions in pyspark , otherwise python may think that they we are using its default values
