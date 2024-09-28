from pyspark.sql.types import StructType,IntegerType,StringType,StructField

schema=StructType([StructField('Person',StringType(),True),StructField('Age',IntegerType(),True)])
# Note: If we do not give the schema , then pyspark will try to derive it automatically

data=[('A',12),('B',14),('A',12),('B',14)]

df=spark.createDataFrame(data,schema)

distinct_df=df.distinct()

print(distinct_df.count())
