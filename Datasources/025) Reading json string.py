df=spark.read.format("json").load("dbfs:/FileStore/people.json")

df.display()

df=spark.read.json("dbfs:/FileStore/people.json")

df.display()
