package com.cmcc.ig.qualityAnalysis

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint

case class CellMro(pci: Int, earfcn: Int, rsrp: Double, rsrq: Double)

case class Mro(
                msisdn: String,
                sEci: String,
                timeToken: String,
                lteScPhr: Double,
                lteScSinrUl: Double,
                sCell: CellMro,
                nCells: List[CellMro],
                quality: Option[Double],
                origin: Array[String]
              ) {
  def toTrainingVector: LabeledPoint = LabeledPoint(quality.get, toVector)

  def toVector: Vector = Vectors.dense(MroUtils.generateFields(this))
}

object TrainingMro {
  def apply(line: String, title: Map[String, Int]): Mro = {
    try {

    }
    catch {
      case e: Exception => None
    }
    val cells = line.split(',')
    new Mro(
      cells(title("msisdn")),
      cells(title("seci")),
      cells(title("timetoken")),
      cells(title("lteScPHR")).toDouble,
      cells(title("lteScSinrUL")).toDouble,
      CellMro(cells(title("spci")).toInt, cells(title("searfcn")).toInt,
        cells(title("srsrp")).toDouble, cells(title("rsrq")).toDouble), // s cell
      generateNCells(cells, title).toList, //n cells
      generateQuality(cells, title),
      null)
  }

  private def generateQuality(cells: Array[String], title: Map[String, Int]): Option[Double] = {
    if (title.contains("quality"))
      None
    try
      Some(cells(title("quality")).toDouble)
    catch {
      case exception: Exception => {
        println(exception.getMessage)
        None
      }
    }
  }

  private def generateNCells(cells: Array[String], title: Map[String, Int]): IndexedSeq[CellMro] = {
    for {i <- 1 to 6
         if cells(title("npci" + i)).trim != ""}
      yield CellMro(
        cells(title("npci" + i)).toInt,
        cells(title("nearfcn" + i)).toInt,
        cells(title("nrsrp" + i)).toDouble,
        cells(title("nrsrq" + i)).toDouble)
  }
}