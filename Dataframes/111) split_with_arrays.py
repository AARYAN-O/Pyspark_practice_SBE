from pyspark.sql.types import StringType,StructType,StructField
from pyspark.sql.functions import split

schema=StructType([StructField("Name",StringType(),True),StructField("Color",StringType(),True)])
data=[("A","1|2|3|4"),("B","5|6|7|8")]

df=spark.createDataFrame(data,schema)

df=df.select("name",split("color","\|"))

df.display()

df.printSchema()

# Here , we are creating an array without actually usin ArrayType()
