from pyspark.sql.functions import overlay

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(overlay("title","movieID",2)).display()

# Overlay helps us to overwrite value present in the movieID column over the value present inside title column after
# the second character.
# Note : The indexing starts from 1 (actually we are not even using indexing)
