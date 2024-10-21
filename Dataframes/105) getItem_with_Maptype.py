from pyspark.sql.types import StructType,StructField,MapType,StringType

schema=StructType([StructField("name",StringType(),True),StructField("properties",MapType(StringType(),StringType()),True)])
data=[("A",{"color":"red","year":"2000"}),("B",{"color":"black","return on investment":200000})]

df=spark.createDataFrame(data,schema)

df.display()

df=df.withColumn("color",df.properties.getItem("color"))
df.display()

# Without using getItem also , it works

df=df.withColumn("year",df.properties["year"])
df.display()
