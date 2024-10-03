# While using udf with sql, we need to first register the udf that is given to us/
# Then create a temporary view for that 
# spark.udf.register (name , function ,returntype)
# note that the register function returns the udf.

# Note : UDFs are error prone and hence the errors in UDFs need to be handled properly.

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def lower_case(s):
    modified_string=s[0].lower()+s[1:]
    return modified_string

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

lower_case_function=udf(lambda z:lower_case(z),StringType())

spark.udf.register("my_function",lower_case_function)

df.createOrReplaceTempView("my_db")

spark.sql("select my_function(title) from my_db").show()

