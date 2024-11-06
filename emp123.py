from spark.Sql import SparkSession

spark =sparkSession.builder.appName('localfile3').getOrCreate

data = [(2,"hanu",123),(1,"suresh",526),(3,"yamshg",797)]

columns = ['id','empname','income']


df = spark.createDataFrame(data , columns)

df.show()




