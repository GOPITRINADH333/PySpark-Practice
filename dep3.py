from spark.sql import sparkSession


spark = sparkSession.builder.appName('gopi').getorCreate

data = [(1,'gopi',55),(2,'haresh',66),(3,'bhargav',77)]

columns = ['id','empname','income']

df = spark.createDataFrame(data , column)

df.show()

