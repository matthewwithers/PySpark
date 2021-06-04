from pyspark import SparkConf, SparkContext  # at a min will need these imports
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)
sc.setLogLevel("FATAL")

"""
textFile breaks up file line by line to fill the RDD,
each line is considered a full string seperated by spaces.
calling the split() method on the string splits into an array,
and we grab the wanted value by its index location.

map() iterates through each line
"""

lines = sc.textFile("../Files/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])

"""
countByValue counts unqiue values in the RDD
"""
result = ratings.countByValue()
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
