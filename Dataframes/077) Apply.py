# How to cause changes to the columns in a dataframe 
# 1)withColumn
# 2) select()
# 3) filter()
# 4) groupBy()
# 5) transform()
# 6) apply
# 7) udfs

# Note : out of this apply method is present in pandas and not pyspark, but we can use it using pyspark.pandas library
# Note : Pandas does not allow distributed computing where as pyspark does
# Hence pyspark is used for large data where as pandas is not 

# We can run pandas API in pyspark as well.

# Axis=0 and axis=1 ==> when an operation is run along axis=0, it means that this operation will work on each columm
# Similiarly when it is applied along axis=1 , it means that this operation will work on each row.

import pyspark.pandas as ps 
df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")
# df.display()
pyspark_df=ps.DataFrame(df)

pyspark_df.display()

def addition(data):
    return data[0]+data[1]

addDF=pyspark_df.apply(addition,axis=1)
print(addDF)

# movieId	title	genres
# 1	Toy Story (1995)	Adventure|Animation|Children|Comedy|Fantasy
# 2	Jumanji (1995)	Adventure|Children|Fantasy
# 3	Grumpier Old Men (1995)	Comedy|Romance
# 4	Waiting to Exhale (1995)	Comedy|Drama|Romance
# 5	Father of the Bride Part II (1995)	Comedy

# 0                                      1Toy Story (1995)
# 1                                        2Jumanji (1995)
# 2                               3Grumpier Old Men (1995)
# 3                              4Waiting to Exhale (1995)
# 4                    5Father of the Bride Part II (1995)
