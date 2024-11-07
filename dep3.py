from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('gopi').getOrCreate()

data = [(1,'gopi',55),(2,'haresh',66),(3,'bhargav',77)]

columns = ['id','empname','income']

df = spark.createDataFrame(data , columns)

df.show()

