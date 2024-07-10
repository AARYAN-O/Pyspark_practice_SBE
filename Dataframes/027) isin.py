# like operator is used in pyspark or sql for regular expressions (regex) for checking if a particular string matches with that 

# isin is used for checking if whatever we want to check is in a particular collection or not.

from pyspark.sql.functions import col

# why do we need to have tuples inside the list for creating dataframes in pyspark.
data=[('Peter',12,100),('Parker',14,200),('Falcon',16,300)]

column_names=['Name','Age','Roll']

Allowed_age_values_in_df=[12,14]

df=spark.createDataFrame(data,column_names)

df1=df.where(col('Age').isin(Allowed_age_values_in_df))

df1.show()
