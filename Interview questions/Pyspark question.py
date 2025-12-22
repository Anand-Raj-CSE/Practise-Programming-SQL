# Question was to generate the domain names from the email column , unique domain names
from pyspark.sql.functions import *
# This is wrong code
df = df.withColumn("Domain",split(split(col(df["email"]),'@')[1],"."[0]))
# After splitting we get the the columns so correct code as below
df = df.withColumn("Domain",split(split(col(df["email"]),'@')[1],"\\.")[0]) 
# Pyspark split uses regex , so \ to escape . and other \ to escape the python \