# arraycontains is actually used for checking if the array present inside a column (if available) contains a particular
# value inside it or not

from pyspark.sql.functions import array_contains,col

data=[('Peter',[10,20,30,40]),('Tony',[50,60,70,80])]

column_names=['Name','Numbers_Assigned']

df=spark.createDataFrame(data,column_names)

df.show()

df1=df.where(array_contains(col('Numbers_Assigned'),40))

df1.show()
