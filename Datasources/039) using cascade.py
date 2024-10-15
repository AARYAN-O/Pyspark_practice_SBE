# In pyspark , the database cannot be dropped if it is containing some data like schema , tables , etc
# So, if we try to delete it (while it is containing the data) , it will give us error.
# But if we use cascade, it will delete the database, as well as internal values within itself.

spark.sql("drop database db2 cascade")

