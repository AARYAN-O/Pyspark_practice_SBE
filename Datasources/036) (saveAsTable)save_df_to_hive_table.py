# We can save the dataframe as an internal table in hive.
# Internal tables are stored in a Hive warehouse directory, and their data files are managed by Hive’s storage handler.
# When you drop an internal table, it drops the data and also drops the metadata of the table.

# Note : The saveAsTable is always used for saving the table inside hive metastore.

data=[(1,"a"),(2,"b")]
schema=["id","person"]
df=spark.createDataFrame(data,schema)

df.write.mode("overwrite").saveAsTable("hive_table_test")
