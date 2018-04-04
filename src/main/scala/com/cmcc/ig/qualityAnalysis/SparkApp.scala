package com.cmcc.ig.qualityAnalysis

import org.apache.spark.{SparkConf, SparkContext}

object SparkApp {
  private def formatResult(result: Double): String = {
    result match {
      case 1.0 => "弱覆盖"
      case 2.0 => "MOD3干扰"
      case 3.0 => "重叠覆盖"
      case 4.0 => "外部干扰"
      case 5.0 => "疑似漏配邻区"
      case 6.0 => "高UE发射功率"
      case _ => ""
    }
  }

  def main(args: Array[String]): Unit = {
    val trainingDir = "data"//args(0)
//    val queryDir = args(1)
//    val outputFile = args(2)

    val conf = new SparkConf().setAppName("quality analysis").setMaster("local")
    val sc = new SparkContext(conf)

    val trainingData = PreProcessor.obtainTrainingData(trainingDir, sc)
    //    val model = QualityLearner.fitModel(trainingData)

    //QualityLearner.testModel(trainingData)

    //    val queryData = PreProcessor.obtainQueryData(queryDir, sc)
    //
    //    val predictedMro = queryData.map(t => {
    //      val result = QualityLearner.predict(model, t._2)
    //      t._1 + "," + formatResult(result)
    //    })
    //
    //    predictedMro.saveAsTextFile(outputFile)
  }
}