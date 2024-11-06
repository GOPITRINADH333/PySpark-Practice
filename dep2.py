from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('local file').getOrCreate()

data = [(1,'goopi',123)]

columns =['id','name','cell no']

df = spark.createDataFrame(data , columns)

df.show()





