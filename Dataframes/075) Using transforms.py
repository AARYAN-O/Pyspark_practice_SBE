# If you are concatenating the literals, you need to use the lit function and the concat.
# Since df is not a string even if the values inside it are of string type , we cannot really do something like df["title"]+"x"
# or for that matter we cannot even do df["title"]+lit("x")

# what is the use of transforms ?
# The use of transforms is to apply multiple functions/transformations on the dataframes one after the other.

# MOST IMPORTANT THING ABOUT THIS IS THAT WE NEED TO APPLY TRANSFORM ONLY ON THOSE COLUMNS THAT CONTAIN VALUES IN 
# THE FORM OF ARRAYS.


# from pyspark.sql.functions import lit,concat,length

# df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

# def concatenate_string(df):
#     return df.withColumn("modified_string",concat(df['title'],lit("h")))

# def counter(df):
#     x="ok"+df.title
#     return df.withColumn("length_of_string",x)


# df.transform(concatenate_string).transform(counter).display()

The above example may have worked because pyspark would have converted it into arrays 

# MOST IMPORTANT THING ABOUT THIS IS THAT WE NEED TO APPLY TRANSFORM ONLY ON THOSE COLUMNS THAT CONTAIN VALUES IN 
# THE FORM OF ARRAYS.

# What is a dataframe ?
# A dataframe is a table like structure consisting of rows and columns.

# Let us try to do it with arrays as well.

data=[("A",[1,2,3,4]),("B",[5,6,7,8])]
schema=["Name","ArrayColumn"]

def concatenate_string(df):
    return df.withColumn("concatenating_string",concat(df['ArrayColumn'],lit(' h')))
    
def len_string(df):
    return df.withColumn("length_of_string",concat(df['ArrayColumn'],length(df['Name'])))
    

df=spark.createDataFrame(data,schema)
df.transform(concatenate_string).transform(len_string).display()





