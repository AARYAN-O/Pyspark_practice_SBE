from pyspark.sql.functions import explode
data=[(1,['A','B']),(2,['C','D'])]
schema=["number","list_of_characters"]

df=spark.createDataFrame(data,schema)

df=df.withColumn("exploded",explode(df.list_of_characters))

df.display()

# Note : explode will create mapping of all the columns (like cross joins) that are included with the first column 
# this will be valid in case , we are using something like this 

# +----------------+------+
# |            name|   col|
# +----------------+------+
# |    James,,Smith|  Java|
# |    James,,Smith| Scala|
# |    James,,Smith|   C++|
# |   Michael,Rose,| Spark|
# |   Michael,Rose,|  Java|
# |   Michael,Rose,|   C++|
# |Robert,,Williams|CSharp|
# |Robert,,Williams|    VB|
# +----------------+------
