# If you are concatenating the literals, you need to use the lit function and the concat.
# Since df is not a string even if the values inside it are of string type , we cannot really do something like df["title"]+"x"
# or for that matter we cannot even do df["title"]+lit("x")


from pyspark.sql.functions import lit,concat

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

def concatenate_string(df):
    return df.withColumn("modified_string",concat(df['title'],lit("h")))

def counter(df):
    x="ok"+df.title
    return df.withColumn("length_of_string",x)


df.transform(concatenate_string).transform(counter).display()
