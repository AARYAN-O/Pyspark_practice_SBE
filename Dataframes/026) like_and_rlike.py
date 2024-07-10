# What is the difference between like and rlike ?
# like only contains % and _ 
# where as rlike contains advanced regex pattern matching as well.

# another important point to note is that rlike is case insensitive and 
# like is case sensitive.


data=[('Peter',12),('Tony',14)]

column_names=['Name','Age']

df=spark.createDataFrame(data,column_names)

df1=df.filter(df.Name.like('_eter'))

df1.show()

df2=df.filter(df.Name.rlike('[a-z]*'))

df.show
