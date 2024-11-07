from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('local file').getOrCreate()

# data = [(1,'goopi',123)]
#
# columns =['id','name','cell no']
#
# df = spark.createDataFrame(data , columns)
#
# df.show()

data = [(1,'gopi',12),(2,"suresh",23),(3,'naresh',56)]

columns = ['id','empname','section']

df = spark.createDataFrame(data , columns)

df.show()







