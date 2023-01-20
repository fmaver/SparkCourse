from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/superHero/Marvel-names.txt")

lines = spark.read.text("file:///SparkCourse/superHero/Marvel-graph.txt")

# we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
# get the mininum number of connections
minConnections = connections.agg(func.min("connections")).first()[0]
print("The miminum number of connections is " + str(minConnections))

# We want to find the least popular superhero
lessPopular = connections.filter(func.col("connections") == minConnections)

# We want to find the name of all the least popular superheros
leastPopularName = lessPopular.join(names, "id")
leastPopularName.sort(func.col("name")).show(leastPopularName.count())
