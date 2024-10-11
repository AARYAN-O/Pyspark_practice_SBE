from pyspark.sql.functions import date_format,current_timestamp

data=[()]
schema=[]

df=spark.createDataFrame(data,schema)

df=df.withColumn("new_column",date_format(current_timestamp(),"dd MM yyyy"))
df.display()

df=df.withColumn("new_column",date_format(current_timestamp(),"dd MM yy"))
df.display()

df=df.withColumn("new_column",date_format(current_timestamp(),"dd/MM/yyyy"))
df.display()

df=df.withColumn("new_column",date_format(current_timestamp(),"dd-MM-yyyy"))
df.display()

df=df.withColumn("new_column",date_format(current_timestamp(),"hh mm ss"))
df.display()

df=df.withColumn("new_column",date_format(current_timestamp(),"hh/mm/ss"))
df.display()

df=df.withColumn("new_column",date_format(current_timestamp(),"hh:mm:ss"))
df.display()

df=df.withColumn("new_column",date_format(current_timestamp(),"hh-mm-ss"))
df.display()

# Note : There is a difference between m and M in pyspark
