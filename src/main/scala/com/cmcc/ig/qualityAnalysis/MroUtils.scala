package com.cmcc.ig.qualityAnalysis

object MroUtils {
  def generateFields(trainMro: Mro): Array[Double] = {
    val co_frequency: ((CellMro, CellMro) => Boolean) = (s, n) => {
      s.earfcn == n.earfcn && (math.abs(s.rsrp - n.rsrp) <= 6)
    }

    Array(
      trainMro.sCell.rsrp, // s rsrp
      trainMro.nCells.count(n => co_frequency(trainMro.sCell, n)), // overlapping cells count
      trainMro.nCells.count(n => co_frequency(trainMro.sCell, n) && trainMro.sCell.pci % 3 == n.pci % 3), // mod 3
      trainMro.lteScPhr,
      trainMro.lteScSinrUl,
      trainMro.nCells.head.rsrp, // max n rsrp
      trainMro.nCells.last.rsrp, // min n rsrp
      trainMro.sCell.rsrp - trainMro.nCells.head.rsrp //diff rsrp
    )
  }

  def formatMro(mro: Mro): String = {

    val formatCell: (CellMro => String) = (cell) => {
      List(cell.earfcn, cell.pci, cell.rsrp) map (_ toString) mkString (",")
    }

    val formattedPrefix = List(mro.origin(0), mro.origin(13), mro.origin(14), mro.origin(15), mro.origin(1), mro.origin(2), mro.origin(3), mro.origin(4),
      mro.origin(5), mro.origin(6), mro.origin(27), mro.origin(28), mro.origin(21), mro.origin(20), mro.origin(16), mro.origin(18), mro.origin(17)) mkString (",")

    //strMro.slice(strMro.indexOf("Mro") + 4, strMro.indexOf("CellMro"))

    val ncells = mro.nCells.zipWithIndex.map(kv => (kv._2, kv._1)).toMap

    val formattedNcells = (for (cell <- mro.nCells) yield formatCell(cell)) mkString (",")

    val suffix = if (mro.nCells.length == 6)
      ""
    else
      "," + ((for (i <- mro.nCells.length until 6) yield ",,,") mkString (","))


    formattedPrefix + "," + formattedNcells + suffix
  }

  def transposeMro(value: Iterable[Array[String]]): Option[Mro] = {
    try {
      val nCells = value.map(c => CellMro(c(11).toInt, c(10).toInt, c(12).toDouble, 0)).toList
        .sortBy(_.rsrp).reverse.take(6)

      val mro = Mro(
        value.head(18),
        value.head(1),
        value.head(3),
        value.head(27).toDouble,
        value.head(28).toDouble,
        CellMro(value.head(5).toInt, value.head(4).toInt, value.head(6).toDouble, 0),
        nCells,
        None,
        value.head
      )
      Some(mro)
    }
    catch {
      case e: Exception => {
        None
      }
    }
  }
}
