import re # regular expressions
from pyspark import SparkConf, SparkContext

# this function is used to normalize the words
def normalizeWords(text):
    # remove all the special characters and return a list of words in lowercase format
    # \W+ means any non-word character (equal to [^a-zA-Z0-9_])
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")

#words = input.flatMap(lambda x: x.split())
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()
wordSorted = sorted(wordCounts.items(), key=lambda x: x[1], reverse=True)

for word, count in wordSorted:
    # if the word can´t be shown, we ignore it
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
