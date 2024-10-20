df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/bank_customer.csv")

df=df.fillna("unknown")

df.display()

# 

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/bank_customer.csv")

df=df.na.fill("unknown")

df.display()

# In both the cases , the na.fill/fillna will be by default applied to all the columns
