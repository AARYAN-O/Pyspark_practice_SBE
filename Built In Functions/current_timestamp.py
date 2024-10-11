from pyspark.sql.functions import current_timestamp

data=[()]
schema=[]

df=spark.createDataFrame(data,schema)

df.withColumn("col_name",current_timestamp()).display()
