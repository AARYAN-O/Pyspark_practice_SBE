# There are two types of aliases in pyspark - 1) ColumnAlias and 2) DataFrameAlias.
# aliases are usually used on the columns and are usually used with select statement.

# In the following example, we have tried to convert the column named 'Name' into 'Naam'

# Column Alias 
from pyspark.sql.types import StructType , IntegerType , StructField 
from pyspark.sql.functions import col

data=[('Peter',10),('Tony',20)]
schema=StructType([
  StructField('Name',StringType(),True),
  StructField('Age',IntegerType(),True)
])

df=spark.createDataFrame(data,schema)

df.select(col('Name').alias('Naam').show()

          # or
      
df.select(df.Name.alias('Naame')).show()

# DataFrame Alias

y=df.alias('New_DF')

y.show()

# DataFrame alias is not so much useful
  
