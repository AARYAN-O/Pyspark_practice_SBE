from pyspark.sql.functions import datediff,current_date

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

df=spark.createDataFrame(data,schema)

df=df.withColumn("new column",current_date()).withColumn("new column2",current_date())

df=df.withColumn("date diff",datediff("new column","new column2"))

df.display()

# Note that the syntax of datediff is like datediff(end_date,start_date)
