from pyspark.sql.types import StringType,StructType,StructField
from pyspark.sql.functions import to_timestamp

# data=[('b',)]
data=[("07 Dec 2021 04:35:05",)]
schema=StructType([StructField("date",StringType(),True)])

df=spark.createDataFrame(data,schema)

df.printSchema()

df=df.withColumn("new_column",to_timestamp("date",format="dd MMM yyyy hh:mm:ss"))

# Note : The format parameter is an optional parameter

# df.display()

df.printSchema()

# timestamp has a specific format only 
# So to_timestamp helps us convert any string into that format.

# See timestamp format at : https://support.thoughtindustries.com/hc/article_attachments/1500010528401/ISODateFormatDiagram.png

