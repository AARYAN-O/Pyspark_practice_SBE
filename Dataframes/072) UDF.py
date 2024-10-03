# UDFs are used to create the User Defined Functions which can be reused.
# Note : with column creates a new column 
# Note : in the below example, we want to try to lower case the first letter 

def convertCase(s):
    res=s[0].lower()+s[1:]
    return res

from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType

convertUDF=udf(lambda z:convertCase(z),StringType())

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df.display()

df.withColumn("lower",convertUDF(df.title)).display()
