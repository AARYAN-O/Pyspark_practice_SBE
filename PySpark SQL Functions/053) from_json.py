from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType,StringType

jsonString="""{"A":1,"B":"2"}"""

data=[(1,jsonString)]
schema=["id","value"]

df=spark.createDataFrame(data,schema)

df.printSchema()

df=df.withColumn("json_col",from_json(df.value,MapType(StringType(),StringType())))

df.display()
