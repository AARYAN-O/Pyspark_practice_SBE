# first define the function
# then convert it into udf using udf function 
# then use it inside withColumn 

# these are the steps that we need to follow for doing creating udfs normally.

# udfs with annotations reduce one step 

from pyspark.sql.types import StringType
@udf(returnType=StringType())
def convertcase(s):
    modified_string=s[0].lower()+s[1:]
    return modified_string

# In the above step only this has already been converted into udf

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")
df.withColumn("new_column",convertcase("title")).display()

