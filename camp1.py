from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName('local file').getOrCreate()

# Sample data
data = [(1, 'gopi', 100), (2, 'sureh', 200), (3, 'naresh', 300), (4, 'hareesg', 400)]

# Column names
columns = ['id', 'emp_name', 'salary']

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()

