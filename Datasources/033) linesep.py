# By default , the line separator is \n , but we can add our own linesep

df=spark.read.format("json").option("header","true").option("multiline","true").option("linesep","|").load("dbfs:/FileStore/*.json")

df.display()
