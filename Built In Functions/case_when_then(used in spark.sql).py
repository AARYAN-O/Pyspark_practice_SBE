df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")
df.createOrReplaceTempView("new_df")

spark.sql("select title,case when title=='Toy Story (1995)' then 'toy story' else 'not toy story' end as determination from new_df ").display()
