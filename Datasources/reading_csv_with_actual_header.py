# use option("header","true") if your data has header as true inisde itself.
# Note : You should also mention whether the header is true or not

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/world_bank_latest.csv")

df.display()
