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

# using dictionaries 

# The below lines of code prove the fact that dictionaries can be used for both na.fill and fillna

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/bank_customer.csv")

df.display()

df=df.na.fill({"initials":"unknown","cust_lname":"unknown"})

df.display()

df=df.fillna({"initials":"unknown","cust_lname":"unknown"})

df.display()
