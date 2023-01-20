from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession. getOrCreate() will return an existing session if one exists, otherwise it will create a new one.
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    # Assume there are four fields per line (ID, name, age, numFriends)
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

# lines is an RDD
lines = spark.sparkContext.textFile("csv-files/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
# schemaPeople is a DataFrame
# we want to run a bunch of different queries on this data, so we cache it in memory
schemaPeople = spark.createDataFrame(people).cache()
# create a temporary view and use it just like a table
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)
  print("---")

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
