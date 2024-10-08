from pyspark.sql.functions import translate

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/movies.csv")

df=df.withColumn('new_movieID',translate('movieId','123','ABC'))

df.display()


# movieId	title	genres	new_movieID
# 1	Toy Story (1995)	Adventure|Animation|Children|Comedy|Fantasy	A
# 2	Jumanji (1995)	Adventure|Children|Fantasy	B
# 3	Grumpier Old Men (1995)	Comedy|Romance	C
# 4	Waiting to Exhale (1995)	Comedy|Drama|Romance	4
# 5	Father of the Bride Part II (1995)	Comedy	5
# 6	Heat (1995)	Action|Crime|Thriller	6
# 7	Sabrina (1995)	Comedy|Romance	7
# 8	Tom and Huck (1995)	Adventure|Children	8
# 9	Sudden Death (1995)	Action	9
# 10	GoldenEye (1995)	Action|Adventure|Thriller	A0
# 11	American President, The (1995)	Comedy|Drama|Romance	AA
# 12	Dracula: Dead and Loving It (1995)	Comedy|Horror	AB
