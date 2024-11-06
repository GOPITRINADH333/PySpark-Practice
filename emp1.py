from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("emp df").getOrCreate()
print("gopi")
print("trinadh")