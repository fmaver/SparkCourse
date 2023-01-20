from pyspark.sql import SparkSession, Row

# create a spark session. getOrCreate() will return an existing session if one exists, otherwise it will create a new one.
spark = SparkSession.builder.appName("test").getOrCreate()
# inputData is a DataFrame
inputData = spark.read.json("myFile.json")
# create a temporary view from the DataFrame
inputData.createOrReplaceTempView("inputData")
# create a new DataFrame from the temporary view using SQL syntax
myResultDataFrame = spark.sql("select foo from bar order by foobar")

# show the result
myResultDataFrame.show()

# get the first row of the result
myResultDataFrame.select("someFieldName")

# filter the result
myResultDataFrame.filter(myResultDataFrame ("someFieldName" > 1))

# group by and aggregate
myResultDataFrame.groupBy("someFieldName").mean()

# convert the DataFrame to an RDD
myResultDataFrame.rdd().map(lambda x: x[0])

# convert the DataFrame to a Pandas DataFrame
from pyspark.sql.types import IntegerType

def square(X):
    return X * X

spark.udf.register("square", square, IntegerType())
df = spark.sql("SELECT square(5) FROM table")