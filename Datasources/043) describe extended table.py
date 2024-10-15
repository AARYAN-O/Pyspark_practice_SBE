# describe extended table is used to get more information on the table .This could be regarding the type of the format
# of the table


# The most important in this is the type of the table , which tells us whether the table is managed table or external table

spark.sql("describe extended my_managed_table").display()
