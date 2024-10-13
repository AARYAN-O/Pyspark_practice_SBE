from pyspark.sql.functions import months_between

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

df=df.withColumn("date_column_1",current_date()).withColumn("date_column_2",current_date()+1)

df=df.withColumn("months between rounded off",months_between("date_column_1","date_column_2",roundOff=True))

df.display()
