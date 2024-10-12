# nullvalue ensures that specified strings are treated as NULL only.

df=spark.read.format("csv").option("header","true").option("nullValue","NA").load("dbfs:/FileStore/test_csv.csv")

df.display()

# Note : Pyspark does not support multiple nullvalues..it supports only single nullvalue

df=spark.read.format("csv").option("header","true").option("nullValue","NA").option("nullValue","NULL").load("dbfs:/FileStore/test_csv.csv")

# Name	Age	City
# John	25	"""""""New York"""""""
# Jane	NA	"""""""San Francisco"""""""
# Jake	null	"""""""Boston"""""""
