import re # regular expressions
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    # remove all the special characters and return a list of words in lowercase format
    # \W+ means any non-word character (equal to [^a-zA-Z0-9_])
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")

#words = input.flatMap(lambda x: x.split())
words = input.flatMap(normalizeWords)

#wordCounts = words.countByValue()
#wordSorted = sorted(wordCounts.items(), key=lambda x: x[1], reverse=True)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
"""
you -> (you, 1) --> (you, 2).... (you, 1878)
"""
# we flip the tuple around so that the count is the key
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
"""
you -> (1878, you)
"""

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
