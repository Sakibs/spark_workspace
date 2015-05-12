#!/usr/bin/env python

import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS

def parseRating(line):
    """
    Parses a rating record in MovieLens format userId::movieId::rating::timestamp .
    """
    # ...

def parseMovie(line):
    """
    Parses a movie record in MovieLens format movieId::movieTitle .
    """
    # ...

def loadRatings(ratingsFile):
    """
    Load ratings from file.
    """
    # ...

def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    # ...

if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print "Usage: [usb root directory]/spark/bin/spark-submit --driver-memory 2g " + \
          "MovieLensALS.py movieLensDataDir personalRatingsFile"
        sys.exit(1)

    # set up environment
    conf = SparkConf() \
      .setAppName("MovieLensALS") \
      .set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)

    # load personal ratings
    myRatings = loadRatings(sys.argv[2])
    myRatingsRDD = sc.parallelize(myRatings, 1)
    
    # load ratings and movie titles

    movieLensHomeDir = '/home/sakib/spark-1.3.1/spark_workspace/data/movieLens'

    # ratings is an RDD of (last digit of timestamp, (userId, movieId, rating))
    ratings = sc.textFile(join(movieLensHomeDir, "ratings.dat")).map(parseRating)

    # movies is an RDD of (movieId, movieTitle)
    movies = dict(sc.textFile(join(movieLensHomeDir, "movies.dat")).map(parseMovie).collect())

    # your code here
    
    # clean up
    sc.stop()
