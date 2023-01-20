# We import SparkContext (sc) which is the fundamental object in Spark. It is the entry point to Spark functionality.
# We import SparkConf which is used to configure SparkContext to tell if I want to run it in one computer, or in a single cluster
from pyspark import SparkConf, SparkContext

# We import collections to sort the results
import collections

# This part is very similar in every spark script
# set master-node as the local machine (not on a cluster)
# set the name of the app. It´s run so quickly so we cannot see it but is a good practise
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

# create the SparkContext object
sc = SparkContext(conf = conf)


# We create the RDD of a fils with the ratings and the RDD looks like:
"""
196 242 3 881250949
186 302 3 891717742
22  377 1 878887116
244  51 2 880606923
166 346 1 886397596
"""
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

# We split the lines and we get the third element of the list which is the rating
ratings = lines.map(lambda x: x.split()[2])

# We count the number of times each rating appears
# {'3': 27145, '1': 6110, '2': 11370, '4': 34174, '5': 21201}
result = ratings.countByValue()

# We sort the results and print them out in a nice way (key, value) -> (rating, number of times it appears) 
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
