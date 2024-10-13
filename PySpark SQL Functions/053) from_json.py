from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType,StringType

jsonString="""{"A":1,"B":"2"}"""

data=[(1,jsonString)]
schema=["id","value"]

df=spark.createDataFrame(data,schema)

df.printSchema()

df=df.withColumn("json_col",from_json(df.value,MapType(StringType(),StringType())))

df.display()

# from json function is used to convert the json string into structType or MapType.
# Note : In python , we can convert almost any datatype into string.
# MapType is used to tell the datatypes of the key-value pairs that are present inside the json string
# In json string , the key can be of one datatype and the value can be of other datatype
# When there are multiple datatypes as values inside json string , its better to write them as stringType.


