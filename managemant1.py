from pyspark.sql import SparkSession

Spark = SparkSession.builder.appName('local file').getOrCreate()

data =[(1,'gopi',100),(2,'sureh',200),(3,'naresh',300),(4,'hareesg',400)]

columns = ['id','emp_name','salary']

df = Spark.CreateDataFrame(data,columns)

df.show()
