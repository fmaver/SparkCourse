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

from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

def parseLine(line):
    values = line.split(",")
    return (int(values[0]), float(values[2]))

lines = sc.textFile("file:///sparkcourse/csv-files/customer-orders.csv")
rdd = lines.map(parseLine)
totalByCustomer = rdd.reduceByKey(lambda x, y: x + y).sortByKey()
results = totalByCustomer.collect()

# now we print the result
for result in results:
    print(str(result[0]) + "\t{:.2f}F".format(result[1]))