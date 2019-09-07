import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
val data = sc.textFile("/input/sparkml/MLInput.csv")

val trainingData = data.map { line =>
val tokens : Array[String] = line.split(",")
val features = Vectors.dense(tokens(1).toDouble)
LabeledPoint(tokens(0).toDouble, features)
}.cache()

val numIterations = 10
val stepSize = 0.00000001
val model = LinearRegressionWithSGD.train(trainingData, numIterations, stepSize)

val predicted = trainingData.map { point =>
val prediction = model.predict(point.features)
(point.label, prediction)
}

predicted.foreach((result) => println(s"predicted label: ${result._1}, actual label: ${result._2}"))


//References:
//https://spark.apache.org/docs/1.6.0/mllib-linear-methods.html
//https://stackoverflow.com/questions/30981471/sparks-linearregressionwithsgd-is-very-sensitive-to-feature-scaling/34168467

