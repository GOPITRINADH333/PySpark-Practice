from pyspark.sql import SparkSession

Spark = SparkSession.builder.appName("sriram").getOrCreate()

data = [(1, "rama", 1000, "hyd", 10),
        (2, "surdhara", 200, "ts", 20),
        (3, "charan", 3000, "kadapa", 30),
        (4, "brhma", 5000, "gnt", 20),
        (5, "srinu", 2000, "andhra", 30),
        (6, "naveen", 5000, "banjara", 30)]

columns = ["id", "name", "sal", "loc", "dept_id"]

df = Spark.createDataFrame(data, schema=columns)

df.show()

Spark = SparkSession.builder.appName("gopi").getOrCreate()

data = [(1, "gopi", 500),
        (2, "ravi", 600),
        (3, "suresh", 700),
        (4, "ramesh", 800)]

columns = ["id", "name", "salary"]

df = Spark.createDataFrame(data, schema=columns)

df.show()

spark = SparkSession.builder.appName("suresh").getOrCreate()

data = [(1, "gopi", 300, "hyd", 5.5), (2, "ramehs", 600, "guntur", 6.6),
        (3, "mundu", 600, "amaravarhi", 4.4)]

columns = ["id", "name", "salary", "location", "ctc"]

df = spark.createDataFrame(data, schema=columns)

df.show()

spark = SparkSession.builder.appName("gopi").getOrCreate()

data = [(11, 'gopi', 500, 'amaravathi', 522007), (12, 'bhargav', 600, 'yerrragondapalem', 522005),
        (13, 'ramesh', 700, 'venkat', 522008),

        (14, 'suresh', 800, 'hyd', 522010), (15, 'sheshu', 900, 'chirla', 522077),
        (16, 'hamesh', 100, 'amara', 522007), ]

columns = ['id', 'name', 'salary', 'location', 'ctc']

df = spark.createDataFrame(data, schema=columns)

df.show()
#
# a = 33
# b = 33
# if b > a:
#   print("b is greater than a")
# elif a == b:
#   print("a and b are equal")
#
#
# a = 200
# b = 33
# if b > a:
#   print("b is greater than a")
# elif a == b:
#   print("a and b are equal")
# else:
#   print("a is greater than b")
#
# x= 100
# y=    200
#
# if x>y:
#     print('x greater than y')
# elif x==y:
#     print('x is equal to y')
#
# a= 50
# b=50
# if a>b:
#     print('a is greater than b')
# elif a==b:
#     print('a is equal to b')
# else:
#     print('a less than b')

x = [1, 2, 3, 4, 5, 6]
x.append(7)
x.extend([8, 9, 10])
x.insert(0, 0)
x.pop(0)
x.remove(1)
x.reverse()
print(x)
x.copy()
print(x)
y = x.sort(reverse=False)
print(y)
y = x.index(5)
print(y)

x.extend([5, 5])
print(x)
y = x.count(5)

print(y)
