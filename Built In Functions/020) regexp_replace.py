from pyspark.sql.functions import regexp_replace

df=df.withColumn('genres',regexp_replace('genres','Adventure','Ad'))

df.display()

# Note : Do not use the wrong spelling as regex_replace
