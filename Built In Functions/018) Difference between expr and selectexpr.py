# Note : expr contains the values within the quotes only.

from pyspark.sql.functions import expr

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.select(expr("title"),expr("genres")).display()

# -----------------------------------------------------------------------------------------

df.selectExpr("title","genres").display()

# So the conclusion is that expr is used on each column where as selectExpr can be used on multiple columns.
