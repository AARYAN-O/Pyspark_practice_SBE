# The types of encodings are :
# 1) UTF-8
# 2) UTF-16
# 3) ISO-8859-1
# 4) ASCII
# 5) utf-7

df=spark.read.format("json").option("multiline","true").option("encoding","UTF-8").option("encoding","UTF-8").load("dbfs:/FileStore/*.json")

df.display()

# ISO-8859-1

df=spark.read.format("json").option("multiline","true").option("encoding","UTF-8").option("encoding","ISO-8859-1").load("dbfs:/FileStore/*.json")

df.display()

# ISO-8859-1

df=spark.read.format("json").option("multiline","true").option("encoding","UTF-8").option("encoding","ASCII").load("dbfs:/FileStore/*.json")

df.display()

# utf-7 and UTF-16 are not working

