# If we do union in which the order of the columns is wrong , then it will directly concatenate without thinking 
# But if we do it correctly with the help of unionByName, then the order of the columsn wont matter.


dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.show(truncate=False)

dept1 = [(10,"Finance"), \
    (20,"Marketing"), \
    (30,"Sales"), \
    (40,"IT") \
  ]
deptColumns = ["dept_id","dept_name"]
deptDF1 = spark.createDataFrame(data=dept1, schema = deptColumns)
deptDF1.show(truncate=False)

unioned_DF=deptDF.union(deptDF1).distinct()
unioned_DF.display()

# Output of the above :
# dept_name	dept_id
# Finance	10
# Marketing	20
# Sales	30
# IT	40
# 10	Finance
# 20	Marketing
# 30	Sales
# 40	IT

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.show(truncate=False)

dept1 = [(10,"Finance"), \
    (20,"Marketing"), \
    (30,"Sales"), \
    (40,"IT") \
  ]
deptColumns = ["dept_id","dept_name"]
deptDF1 = spark.createDataFrame(data=dept1, schema = deptColumns)
deptDF1.show(truncate=False)

unioned_DF=deptDF.unionByName(deptDF1)
unioned_DF.display()



