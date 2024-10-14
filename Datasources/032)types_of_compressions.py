# The types of codecs available in pyspark are -  bzip2, deflate, uncompressed, lz4, gzip, snappy, none.

# Snappy compression

df=spark.read.format("json").option("header","true").option("multiline","true").option("compression","snappy").load("dbfs:/FileStore/*.json")

df.display()

# Gzip compression 

df=spark.read.format("json").option("header","true").option("multiline","true").option("compression","gzip").load("dbfs:/FileStore/*.json")

df.display()

# LZ4 compression

df=spark.read.format("json").option("header","true").option("multiline","true").option("compression","lz4").load("dbfs:/FileStore/*.json")

df.display()

# bzip2 compression

df=spark.read.format("json").option("header","true").option("multiline","true").option("compression","bzip2").load("dbfs:/FileStore/*.json")

df.display()
