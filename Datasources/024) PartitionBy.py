df=spark.read.option("header","true").csv("dbfs:/FileStore/superstore.csv")
df.display()

df.write.partitionBy("Country").mode("overwrite").csv("dbfs:/output/test_csv.csv")

df.display()

# The partitions will already get created in the catalog => dbfs =>filestore=> output file => you will find list of 
# countries because of parititions.

# Note that the partitionBy will make sure that the data structure from row to columnar style.

# Original data :

# ID	OrderID	OrderDate	ShipDate	ShipMode	CustomerID	CustomerName	Segment	City	State	Country	PostalCode	Market	Region	ProductID	Category	Sub-Category	ProductName	Sales	Quantity	Discount	Profit	ShippingCost	OrderPriority
# 32298	CA-2012-124891	31/07/2012	31/07/2012	Same Day	RH-19495	Rick Hansen	Consumer	New York City	New York	United States	10024	US	East	TEC-AC-10003033	Technology	Accessories	Plantronics CS510 - Over-the-Head monaural Wireless Headset System	2309.65	7	0	762.1845	 933.57 	Critical
# 26341	IN-2013-77878	05/02/2013	07/02/2013	Second Class	JR-16210	Justin Ritter	Corporate	Wollongong	New South Wales	Australia	null	APAC	Oceania	FUR-CH-10003950	Furniture	Chairs	Novimex Executive Leather Armchair	 Black	3709.395	9	0.1	-288.765	 923.63 
# 25330	IN-2013-71249	17/10/2013	18/10/2013	First Class	CR-12730	Craig Reiter	Consumer	Brisbane	Queensland	Australia	null	APAC	Oceania	TEC-PH-10004664	Technology	Phones	Nokia Smart Phone	 with Caller ID	5175.171	9	0.1	919.971	 915.49 

# After Partition By :

# 26341	IN-2013-77878	05/02/2013	07/02/2013	Second Class	JR-16210	Justin Ritter	Corporate	Wollongong	New South Wales	_c10	APAC	Oceania	FUR-CH-10003950	Furniture	Chairs	Novimex Executive Leather Armchair	Black	3709.395	9	0.1	-288.765	923.63
# 25330	IN-2013-71249	17/10/2013	18/10/2013	First Class	CR-12730	Craig Reiter	Consumer	Brisbane	Queensland	null	APAC	Oceania	TEC-PH-10004664	Technology	Phones	Nokia Smart Phone	with Caller ID	5175.171	9	0.1	919.971	915.49
# 22732	IN-2013-42360	28/06/2013	01/07/2013	Second Class	JM-15655	Jim Mitchum	Corporate	Sydney	New South Wales	null	APAC	Oceania	TEC-PH-10000030	Technology	Phones	Samsung Smart Phone	with Caller ID	2862.675	5	0.1	763.275	897.35
