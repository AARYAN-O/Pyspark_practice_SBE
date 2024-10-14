# When we do not mention schema , it may happen that pyspark considers the datatype of something as x , but it should be 
# something else.
# In that case, we need to specifically mention the datatype in the schemas.

from pyspark.sql.types import StructType,IntegerType,StructField,StringType

datatypes=StructType([StructField("City",StringType(),True),
                      StructField('RecordNumber',IntegerType(),True),
                      StructField("State",StringType(),True),
                      StructField("ZipCodeType",StringType(),True),
                      StructField("Zipcode",IntegerType(),True),
                      StructField("Name",StringType(),True)])

df=spark.read.format("json").option("multiline","true").schema(datatypes).load("dbfs:/FileStore/*.json")

df.printSchema()

df.display()
