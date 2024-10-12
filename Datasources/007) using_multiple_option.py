# Note that in options , we use the key-value parirs where as in option , we ise the commas to separate key and value

df=spark.read.option("header","True").option("delimiter",",").option("inferSchema","True").csv("dbfs:/FileStore/movies.csv")

df.display()
