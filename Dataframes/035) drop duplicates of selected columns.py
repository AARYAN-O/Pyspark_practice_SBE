from pyspark.sql.types import StructType,StructField,IntegerType,StringType

schema=StructType([StructField('Person',StringType(),True),StructField('Roll_No',IntegerType(),True),StructField('City',StringType(),True),
                   StructField('Favourite_Number',IntegerType(),True)])

data=[('Person1',12,'Los Angles',144),('Person2',14,'Miami',132),('Person3',14,'Miami',132),('Person2',14,'Miami',132)]

df=spark.createDataFrame(data,schema)

df1=df.dropDuplicates(['City','Favourite_Number'])

df1.show()
