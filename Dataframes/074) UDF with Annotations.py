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

# What are the disadvantages of using UDFs?
# Its always best practice to check for null inside a UDF function rather than checking for null outside.
# In any case, if you can’t do a null check in UDF at lease use IF or CASE WHEN to check for null and call UDF conditionally.
# UDFs are a black box to PySpark hence it can’t apply optimization and you will lose all the optimization PySpark does on Dataframe/Dataset.
# When possible you should use Spark SQL built-in functions as these functions provide optimization. Consider creating UDF only when the existing built-in SQL 
# function doesn’t have it.

