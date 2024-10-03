# map function is used to apply a function to each and every value of the dataframe.

def func(x):
    x=x+"h"
    return x

rdd2=rdd.map(lambda x: func(x))
print(rdd2.collect())
