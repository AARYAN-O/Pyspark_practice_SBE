from pyspark.sql.functions import lead
from pyspark.sql.window import Window

data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
]
 
columns= ["employee_name", "department", "salary"]

window_spec=Window.partitionBy("department").orderBy("Salary")

df=spark.createDataFrame(data,columns)

df=df.withColumn("lead",lead("salary",2).over(window_spec))

df.display()
