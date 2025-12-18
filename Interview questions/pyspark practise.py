# Given an json , define schema for same
from pyspark.sql.types import * 
schema = StructType([
    StructField("id",IntegerType(),False),    # this field could not accept NULL values
    StructField("name",StringType(),False),
    StructField("email",StringType(),True)
]
)