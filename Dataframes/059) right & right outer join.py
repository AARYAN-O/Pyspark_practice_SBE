emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.show(truncate=False)

joined_df1=deptDF.join(empDF,deptDF.dept_id==empDF.emp_dept_id,"right")
joined_df1.display()

joined_df2=deptDF.join(empDF,empDF.emp_dept_id==deptDF.dept_id,"rightouter")
joined_df2.display()

# note that the above does not give equal because in pyspark, the value inside the dataframes is not compared.
# Rather the objects are compared.
