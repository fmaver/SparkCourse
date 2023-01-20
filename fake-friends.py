from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# We create the RDD of an online profile and the RDD looks like:
"""
ID, Name, Age, Friends
1, Will, 33, 385
2, Jean-Luc, 26, 2
3, Hugh, 55, 221
4, Deanna, 40, 465
"""
lines = sc.textFile("file:///SparkCourse/csv-files/fakefriends.csv")

# We split the lines and get a tuple of age, numFriends
rdd = lines.map(parseLine)
#print(rdd.collect())
# Output is key/value pairs of (age, numFriends)
"""
33, 385
26, 2
55, 221
40, 465
"""

# We sum the number of friends by age
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#                 33, (385,1)             
#                 33, (2,1)        --> 33, (385,1) + 33, (2,1) = 33, (387,2)  
# and so until all ages are grouped.

# We calculate the average number of friends by age
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])     
# x[1] is the amount of ocurrencies for the same age
# x[0] is the sum of the number of friends for the same age
# 33, (387,2) --> 33, 387/2 = (33, 193.5)         

results = averagesByAge.sortByKey().collect()
for result in results:
    print(result)

 