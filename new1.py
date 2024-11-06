from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('gopi') .getOrCreate()

data = [(11,'hoppish',545202),(17,'gopri',5202),
       (14,'gopfi',52002),(15,'gopji',52020),]

column = {'no', 'name', 'pincode'}

df = spark.createDataFrame(data, schema = column)

df.show()


