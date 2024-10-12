option={
    "header":"True",
    "inferSchema":"True"
}

df=spark.read.options(**option).csv("dbfs:/FileStore/movies.csv")

df.display()
