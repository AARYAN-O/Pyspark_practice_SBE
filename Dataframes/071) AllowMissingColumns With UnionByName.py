dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.show(truncate=False)

dept1 = [(10,"Finance",300), \
    (20,"Marketing",302), \
    (30,"Sales",304), \
    (40,"IT",306) \
  ]
deptColumns = ["dept_id","dept_name","roll_no"]
deptDF1 = spark.createDataFrame(data=dept1, schema = deptColumns)
deptDF1.show(truncate=False)

unioned_DF=deptDF.unionByName(deptDF1,allowMissingColumns=True)
unioned_DF.display()

# dept_name	dept_id	roll_no
# Finance	10	
# Marketing	20	
# Sales	30	
# IT	40	
# Finance	10	300
# Marketing	20	302
# Sales	30	304
# IT	40	306
