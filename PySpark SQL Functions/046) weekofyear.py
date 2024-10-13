from pyspark.sql.functions import weekofyear,current_date

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

df=df.withColumn("date column",current_date())

df=df.withColumn("week of the year",weekofyear("date column"))

df.display()
