dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.show(truncate=False)

dept1 = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF1 = spark.createDataFrame(data=dept1, schema = deptColumns)
deptDF1.show(truncate=False)

unioned_DF=deptDF.unionAll(deptDF1)
unioned_DF.display()

# Union and Union All works exactly in the same way - no difference at all.
