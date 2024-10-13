from pyspark.sql.functions import trunc,current_date

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

df=df.withColumn("truncated_column",trunc("date column","month"))

df.display()

# trunc helps us define the granularity of dates.
# So for example , if we set the granularity to month , that means that the date column will change to the first 
# day of every month and then the month and year column will change continuosly.
