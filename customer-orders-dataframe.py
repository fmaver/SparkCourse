# We have
"""
44,8602,37.19
35,5368,65.89
44,3391,40.64
47,6694,14.98
35,680,13.08
"""

# We want
"""
44,77.83
35,78.97
47,14.98
"""

# customer_id, item_id, amount_spent

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

schema = StructType([ \
                        StructField("customer_id", IntegerType(), True), \
                        StructField("item_id", IntegerType(), True), \
                        StructField("amount_spent", FloatType(), True)])

df = spark.read.schema(schema).csv("file:///SparkCourse/csv-files/customer-orders.csv")
df.printSchema()

# We want to find the total amount spent by each customer
cust_amount = df.select("customer_id", "amount_spent")
cust_amount_sorted = cust_amount.groupBy("customer_id").agg(func.round(func.sum("amount_spent"), 2).alias("total_spent")).sort("total_spent")
cust_amount_sorted.show(cust_amount_sorted.count())

spark.stop()