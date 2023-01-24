from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open("C:/SparkCourse/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("ALSExample").getOrCreate()
    
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

print("Loading movie names...")

names = loadMovieNames()
    
ratings = spark.read.option("sep", "\t").schema(moviesSchema) \
    .csv("file:///SparkCourse/ml-100k/u.data")

# we print ratings
ratings.show()

print("Training recommendation model...")

# another way to set parameters
# als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")

# We set the hyperparemeter regParam to 0.01, We set the rank to 10
# We set the maxIter to 5 & We set the implicitPrefs to False
# which are the default in the API.
als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID") \
    .setRatingCol("rating")

# Fit the model to the data by calling the fit() method
# fit() method returns an ALSModel object
model = als.fit(ratings)

# Manually construct a dataframe of the user ID's we want recs for
userID = int(sys.argv[1]) #Get the ID from the command line. 1st argument. 0 is the script name
userSchema = StructType([StructField("userID", IntegerType(), True)])
users = spark.createDataFrame([[userID,]], userSchema)

# Generate top 10 movie recommendations for each user
recommendations = model.recommendForUserSubset(users, 10).collect()

print("Top 10 recommendations for user ID " + str(userID))

for userRecs in recommendations:
    myRecs = userRecs[1]  # userRecs is (userID, [Row(movieId, rating), Row(movieID, rating)...])
    for rec in myRecs: # my Recs is just the column of recs for the user
        movie = rec[0] # For each rec in the list, extract the movie ID and rating
        rating = rec[1]
        movieName = names[movie]
        print(movieName + str(rating))
        

