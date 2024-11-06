from pyspark.sql import SparkSession
from pyspark.sql.functions import column

spark = SparkSession.builder.appName("local file").getOrCreate()


columns= ['id','name','salary']

df = spark.createDataFrame(data,columns)


