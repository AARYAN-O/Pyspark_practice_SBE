# Note that here we need to put the list of strings

df=spark.read.csv(["dbfs:/FileStore/world_bank_latest.csv","dbfs:/FileStore/movies.csv"])

# this will merge the columns horizontally
df.display()
