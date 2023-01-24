# Use cases for python files:

### RDD´s exercises using pySpark:

ratings-counter.py: example on how to confugure the spark environment and create our first RDD with a TextFile

fake-friends.py: example on how to create a RDD with a list of tuples and how to use the mapValue(), reduceByKey() and sortByKey() methods

min-temperatures.py and max-temperatures.py: example on how to create a RDD with a list of tuples and how to use the filter(), map() and reduce() methods

word-count-sorted.py: example on how to use the flatmap() and sorted() and sort gettting a python object.

word-count-better-sorted.py: example on how to use the flatmap() and sorting it the old way to sort in the RDD instead of just getting a python object.

customer-orders-sorted-key.py and customer-orders-sorted-value.py: Exercise to practise al above

-----

### DataFrames exercises using SparkSQL

spark-sql-example.py: example on how to create a DataFrame from a RDD and how to use the select() and show() methods

spark-sql.py: example on how to create a DataFrame from a RDD and how to use the groupBy() and count() methods

spark-sql-dataframe.py: example on create directly not only using SQL commands, but using straight up code.

friends-by-age-dataframe.py: use module function (func) and more features to improve the code. (groupby, agg, sort, show, func.avg, func.round)

word-count-better-sorted-dataframe.py: use inputDF with select, func.explode (similar to flatmap), func.split, alias, filter, func.lower, groupBy, count, sort, show, count.

min-temperatures-dataframe.py: we create the schema for the df using StructType and StructField.
we also use filter and select, and use withColumn to situate in a particual column and add some operations to it.

customer-orders-dataframe.py: we create our own schema for the df using StructType and StructField.
we also use group by, func.round, func.sum, sort, show, count.


-----

### Advances examples of Spark Programs

popular-movies-dataframe.py: we create our schema and check the most 10 popular movideId´s by using groupBy, count, sort, show, count.

popular-movies-nice-dataframe.py: we want now to know which movie actually correspond to that id. So we use broadcast and udf function to get the name of the movie and distribute it to all the nodes.

most-popular-superhero-dataframe.py: we want to know which superhero is the most popular. We use withColumn, func.trim, func.split, alias, filter, sort, select, first to get from the most popular movies (movieId | count) the movieName from (movieName | movieID)

most-obscure-superhero-dataframe.py: we want to know which superhero is the most obscure. We use join, withColumn, func.trim, func.split, alias, filter, sort, select, first to get from the most popular movies (movieId | count) the movieName from (movieName | movieID)

degrees-of-separation.py: we want to know the degrees of separation between two superheroes. We use sc.accumulator(), flatMap(), reduceByKey(). And for these 2 last methods, we choose to use RDD instead of DataFrames because the algorithm used (BFS) lent itself very well using map and reduce (RDD methods). In this case a DataFrame would be slower.

movie-similarities-dataframe.py: we use a particular to determine the most rating film. We use selfJoin to match every 2 films aboiding duplicates. we filter, select & collect to get a movie name. We use an entry in the cmd when doing spark-submit ...py filmID to enter the film we want to recomende similars movies.


MovieSimilarities1M.py: We modify the movie-similarities-dataframe.py to be able to place it in the cloud. We use partitionBy to let use more executors to distribute the task

-----

### Machine Learning with Spark

movie-recommendations-als-dataframe.py: we use the ALS algorithm to recommend movies to a user. We use the same dataset as the previous exercise. We use train, als, fit, recommendForUserSubset, etc.
    We hardcode the u.data freom the ml-100k to use it as a case study. We said that the user 0 loves Star Wars and The Empire Striker Back, but hated the movie Gone with the Wind

spark-linear-regression.py: we use the LinearRegression algorithm to predict the price of a house.

real-estate.py: we use Decision Trees to predict the price of a house.


-----|

### Spark Streaming

