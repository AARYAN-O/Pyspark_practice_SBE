from pyspark.sql.functions import regexp_replace

df=df.withColumn('genres',regexp_replace('genres','Adventure','Ad'))

df.display()

# Note : Do not use the wrong spelling as regex_replace

# characters for replacement. If this is shorter than matching string then those chars that don’t have replacement 
# will be dropped.
