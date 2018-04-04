package com.cmcc.ig.qualityAnalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

object QualityLearner {
  def cvTest(data: RDD[LabeledPoint]): Unit = {
    (1 to 5).foreach(i => {
      println(i)
      testModel(data)
    })
  }

  def testModel(data: RDD[LabeledPoint]): Unit = {
    val splits = data.randomSplit(Array(0.7, 0.3))

    val (trainingData, testData) = (splits(0), splits(1))

    val model = fitModel(trainingData)

    val testErr = testData.map(p => {
      val prediction = model.predict(p.features)
      if (p.label == prediction) 1 else 0
    }).mean

    println(testErr)
  }

  def predict(model: RandomForestModel, feature: Vector): Double = model.predict(feature)

  def fitModel(trainingData: RDD[LabeledPoint]): RandomForestModel = {
    //val treeStrategy = Strategy.defaultStrategy("Classification")
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numClasses = 7
    val numTrees = 8
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 100

    //RandomForest.trainClassifier(trainingData, treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)
    RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy,
      impurity, maxDepth, maxBins)
  }
}