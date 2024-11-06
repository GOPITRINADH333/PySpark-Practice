from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()
data = [
    {"name": "John", "age": 30, "city": "New York"},
    {"name": "Anna", "age": 25, "city": "London"},
    {"name": "Mike", "age": 35, "city": "San Francisco"}
]


# Create DataFrame
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()