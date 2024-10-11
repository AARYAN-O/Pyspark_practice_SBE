from pyspark.sql.functions import current_timestamp,current_date

date=[()]
schema=[]

df=spark.createDataFrame(data,schema)

spark.sql("select current_timestamp()").display()

spark.sql("select current_date()").display()
