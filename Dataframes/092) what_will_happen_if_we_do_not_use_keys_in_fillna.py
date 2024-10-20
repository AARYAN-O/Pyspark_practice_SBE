# Note that we do not need to import fillna or na.fill

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")

df.display()

df=df.fillna("unknown",["PostalCode"])

df.display()

df=df.na.fill("unknown",["PostalCode"])

print("second")

df.display()

# Note : while using fillna , its just replacing the first element of the PostalCode column with unknown ,
# where as while using na.fill , its replacing all the elements of the PostalCode column
