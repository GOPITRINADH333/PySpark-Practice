from pyspark.sql import SparkSession
from pyspark.sql.functions import column

spark = SparkSession.builder.appName("local file").getOrCreate()

data =[(1,'gopi',500),(2,"suri",600)]

columns= ['id','name','salary']

df = spark.createDataFrame(data,columns)

df.show()

