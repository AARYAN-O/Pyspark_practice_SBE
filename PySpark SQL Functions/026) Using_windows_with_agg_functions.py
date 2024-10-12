from pyspark.sql.functions import col,min,max,avg,sum,row_number
from pyspark.sql.window import Window 


data = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )

schema= ["employee_name", "department", "salary"]

# Note : We can even use the tuples instead of list for writing data.

window_spec=Window.partitionBy("department").orderBy("salary")

df=spark.createDataFrame(data, schema)

df=df.withColumn("max",max(col("salary")).over(window_spec))\
.withColumn("min",min(col("salary")).over(window_spec))\
.withColumn("avg",avg(col("salary")).over(window_spec))\
.withColumn("sum",sum(col("salary")).over(window_spec))

df=df.withColumn("row number",row_number().over(window_spec)).filter(col("row number")==1).select("employee_name",\
"department","salary","max","min","avg","sum")

df.display()
