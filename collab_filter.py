from pyspark import SparkConf, SparkContext

from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import Rating


conf = SparkConf().setMaster("local").setAppName("Collaborative Filtering")
sc = SparkContext(conf = conf)

datapath = 'spark_workspace/data/test.data'

data = sc.textFile(datapath)
ratings = data.map(lambda l: l.split(',')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

print "--- Training"

rank = 10
numIterations = 100
model = ALS.train(ratings, rank, numIterations)

print "--- Predicting"

testdata = ratings.map(lambda l: (l[0], l[1]))
predictions = model.predictAll(testdata).map(lambda l: ((l[0], l[1]), l[2]))
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).reduce(lambda x, y: x + y)/ratesAndPreds.count()
print("Mean Squared Error = " + str(MSE))

"""
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

// Load and parse the data
val data = sc.textFile("spark_workspace/data/test.data")
val ratings = data.map(_.split(',') match {
    case Array(user, item, rate) =>  Rating(user.toInt, item.toInt, rate.toDouble)
})

// Build the recommendation model using ALS
val numIterations = 20
val model = ALS.train(ratings, 1, 20, 0.01)

// Evaluate the model on rating data
val usersProducts = ratings.map{ case Rating(user, product, rate)  => (user, product)}
val predictions = model.predict(usersProducts).map{
    case Rating(user, product, rate) => ((user, product), rate)
}
val ratesAndPreds = ratings.map{
    case Rating(user, product, rate) => ((user, product), rate)
}.join(predictions)
val MSE = ratesAndPreds.map{
    case ((user, product), (r1, r2)) =>  math.pow((r1- r2), 2)
}.reduce(_ + _)/ratesAndPreds.count
println("Mean Squared Error = " + MSE)
"""