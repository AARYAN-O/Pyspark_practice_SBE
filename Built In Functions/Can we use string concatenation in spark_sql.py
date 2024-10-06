df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.createOrReplaceTempView("empdf")

spark.sql("select title " + "from empdf").display()
