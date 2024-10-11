df=spark.read.csv(["dbfs:/FileStore/world_bank_latest.csv","dbfs:/FileStore/movies.csv"])

# this will merge the columns horizontally
df.display()
