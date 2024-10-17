from pyspark.sql.functions import explode
data=[(1,['A','B']),(2,['C','D'])]
schema=["number","list_of_characters"]

df=spark.createDataFrame(data,schema)

df=df.withColumn("exploded",explode(df.list_of_characters))

df.display()