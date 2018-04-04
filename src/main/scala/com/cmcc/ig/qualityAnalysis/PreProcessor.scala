package com.cmcc.ig.qualityAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector

object PreProcessor {
  //def obtainTrainingData(dir: String, sc: SparkContext): RDD[LabeledPoint] = {
  def obtainTrainingData(dir: String, sc: SparkContext) = {
    val fileName = dir + "/quality_training.csv"
    val trainingRdd = sc.textFile(fileName)
    val trainingTitle = trainingRdd.first.split(',').zipWithIndex.toMap
    trainingRdd.foreach{x=>println(x (2))}//filter(!_ (0).isLetter).
//      .map(TrainingMro(_, trainingTitle).toTrainingVector)
  }

  def obtainQueryData(dir: String, sc: SparkContext): RDD[(String, Vector)] = {
    val files = dir + "/*"

    sc.textFile(files)
      .map(_.split(1.toChar))
      .filter(cells => cells.length == 29 && cells(24) == "1" && cells(25) == "1")
      .map(cells => {

        val key = Array(cells(1), cells(2), cells(3), cells(18), cells(20), cells(21), cells(27), cells(28)).mkString("_")
        (key, cells.mkString(","))
      })
      .groupByKey
      .map(kv => MroUtils.transposeMro(kv._2.toList.distinct.map(c => c.split(','))))
      .filter(_.nonEmpty)
      .map(m => (MroUtils.formatMro(m.get), m.get.toVector))
  }
}