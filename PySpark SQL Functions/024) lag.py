from pyspark.sql.window import Window
from pyspark.sql.functions import lag,col

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

df=spark.createDataFrame(data,columns)

window_spec=Window.partitionBy("department").orderBy("salary")

df=df.withColumn("lag",lag("salary",2).over(window_spec))

df.display()
