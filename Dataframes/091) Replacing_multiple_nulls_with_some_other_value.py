# using fillna

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/bank_customer.csv")

df.display()

df=df.fillna("unknown",subset=["Initials","Cust_lname"])

df.display()

# using na.fill()


df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/bank_customer.csv")

df.display()

df=df.na.fill("unknown","Initials").na.fill("unknown","cust_lname")

df.display()
