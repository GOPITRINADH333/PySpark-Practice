from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(' hadoop').getOrCreate()

data  = [(1,'suresh',23),(2,'haresh',36),(3,'dinesh',32)]

columns = ['id','name','sectiom']

df = spark.createDataFrame( data , columns)

df.show()



