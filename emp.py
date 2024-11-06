from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sriram") .getOrCreate()
#
# data = [("gopi", 34,9000), ("rama", 45,50000), ("srinu", 29,80000)]
# columns = ["Name", "Age","sal"]
#
# df_emp = spark.createDataFrame(data, schema=columns)
#
# df_filtered = df_emp.filter(df_emp.Age > 30)
# df_sorted = df_filtered.sort("Age")
#
# df.show()
#




data = [("bhargav",32,45000),("prasanth",31,35000),("gopi",30,25000)]
columns = ["name","age","salary"]

df_emp1 = spark.createDataFrame(data,schema=columns)

df_emp1.count()













