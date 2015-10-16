from pyspark import SparkConf, SparkContext

from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import Rating


conf = SparkConf().setMaster("local").setAppName("Collaborative Filtering")
sc = SparkContext(conf = conf)

print '**** Loading data ****'
datapath = 'spark_workspace/data/movieLens/u.data'

part = 4

data = sc.textFile(datapath, part)
ratings = data.map(lambda l: l.split('\t')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

print '**** Starting ALS regression ****'
rank = 50
numIterations = 100
print '**** rank: ' + str(rank) + ', nIters: ' +str(numIterations) + '  ****'

model = ALS.train(ratings, rank, numIterations)

print '**** Finished regression, getting predictions ****'
testdata = ratings.map(lambda l: (l[0], l[1]))
predictions = model.predictAll(testdata).map(lambda l: ((l[0], l[1]), l[2]))

# print predictions.collect()
# print "-------------------"
# print ratings.collect()
# print "-------------------"
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
# print ratesAndPreds.collect()

MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).reduce(lambda x, y: x + y) / ratesAndPreds.count()
# print MSE.toDebugString()

print("Mean Squared Error = " + str(MSE))

