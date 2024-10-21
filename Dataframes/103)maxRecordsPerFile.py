# This function is used when there is a lot of skewness expected in the data.
# This function controls the number of records within each partition so as to prevent skewness in the partitions.

# dont use option("mode","overwrite") .Rather just use mode("overwrite")

df=spark.read.format("csv").option("header","true").load("dbfs:/FileStore/superstore.csv")

df.write.partitionBy("category").mode("overwrite").option("maxRecordsPerFile",4).csv("dbfs:/output/new_data.csv")

new_df=spark.read.format("csv").load("dbfs:/output/new_data.csv")

new_df.display()

new_data_frame=spark.read.format("csv").load("dbfs:/output/new_data.csv/category=Furniture/part-00000-tid-2306519763801255159-8d81ede8-5088-4820-8960-8d70b9fa9962-8-1.c000.csv")

new_data_frame.display()



# _c0	_c1	_c2	_c3	_c4	_c5	_c6	_c7	_c8	_c9	_c10	_c11	_c12	_c13	_c14	_c15	_c16	_c17	_c18	_c19	_c20	_c21	_c22
# 26341	IN-2013-77878	05/02/2013	07/02/2013	Second Class	JR-16210	Justin Ritter	Corporate	Wollongong	New South Wales	Australia	null	APAC	Oceania	FUR-CH-10003950	Chairs	Novimex Executive Leather Armchair	Black	3709.395	9	0.1	-288.765	923.63
# 30570	IN-2011-81826	07/11/2011	09/11/2011	First Class	TS-21340	Toby Swindell	Consumer	Porirua	Wellington	New Zealand	null	APAC	Oceania	FUR-CH-10004050	Chairs	Novimex Executive Leather Armchair	Adjustable	1822.08	4	0	564.84	894.77
# 31192	IN-2012-86369	14/04/2012	18/04/2012	Standard Class	MB-18085	Mick Brown	Consumer	Hamilton	Waikato	New Zealand	null	APAC	Oceania	FUR-TA-10002958	Tables	Chromcraft Conference Table	Fully Assembled	5244.84	6	0	996.48	878.38
# 40936	CA-2012-116638	28/01/2012	31/01/2012	Second Class	JH-15985	Joseph Holt	Consumer	Concord	North Carolina	United States	28027	US	South	FUR-TA-10000198	Tables	Chromcraft Bull-Nose Wood Oval Conference Tables & Bases	4297.644	13	0.4	-1862.3124	865.74	Critical
