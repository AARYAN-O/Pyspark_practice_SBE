empyRDD=spark.sparkContext.parallelize([])
from pyspark.sql.types import StructField,StructType,StringType

schema=StructType(
    [
        StructField('firstname',StringType(),True),
        StructField('middlename',StringType(),True),
        StructField('lastname',StringType(),True)
    ]
)

df1=spark.createDataFrame([],schema)

df1.printSchema()
