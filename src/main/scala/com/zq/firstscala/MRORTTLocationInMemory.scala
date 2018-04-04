package com.zq.firstscala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import com.cmcc.ig.qualityAnalysis.Mro
import org.apache.spark.util.SizeEstimator
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SaveMode
import org.apache.spark.broadcast.Broadcast


object MRORTTLocationInMemory {
  case class Mro(objid: String, timestamp: String, s1apid: String, scrsrp: String, ncci: String,  ncrsrp: String)
  case class TrainingMro(objid: String, lonlat: String, scrsrp: String, ncci_list: String, ncrsrp_list: String, grid_tag:String)
  case class MainRegionEci(objid: String, city: String)
//  case class MRORTT(vendor_name: String,
//                    enbid: String,
//                    objid: String, --2
//                    objmmeues1apid: String, --3
//                    objmmegroupid: String,
//                    objmmecode: String,
//                    objtimestamp: String, --6
//                    gsmncellbcc: String,
//                    gsmncellbcch: String,
//                    gsmncellcarrierrssi: String,
//                    gsmncellncc: String,
//                    ltencearfcn: String,
//                    ltencpci: String,
//                    ltencrsrp: String, --13
//                    ltencrsrq: String,
//                    ltescaoa: String,
//                    ltescbsr: String,
//                    ltescearfcn: String,
//                    ltescenbrxtxtimediff: String,
//                    ltescpci: String,
//                    ltescpdschprbnum: String,
//                    ltescphr: String,
//                    ltescpuschprbnum: String,
//                    ltescri1: String,
//                    ltescri2: String,
//                    ltescri4: String,
//                    ltescri8: String,
//                    ltescrip: String,
//                    ltescrsrp: String, --28
//                    ltescrsrq: String,
//                    ltescsinrul: String,
//                    ltesctadv: String,
//                    region_name: String,
//                    city_name: String,
//                    ltencci: String, --34
//                    ifncci: String,
//                    dis: String,
//                    is_downtown: String,
//                    zone_id: String,
//                    mroseq: String,
//                    cell_lon: String,
//                    cell_lat: String)
  def rddToMroDFCase(sparkSession : SparkSession, path:String, mainobjid_broadcast: Broadcast[scala.collection.Map[String,String]]):Dataset[Mro] = {
    val rowsRDD = sparkSession.sparkContext.textFile(path)
    val mroRDD = rowsRDD.map(row => row.split(",")).filter(_.size == 42).filter(row => mainobjid_broadcast.value.contains(row(2)))
                    .map{col => Mro(col(2),col(6),col(3),col(28),col(34),col(13))}
       //导入隐式操作，否则RDD无法调用toDF方法
    import sparkSession.implicits._
    val mroDS = mroRDD.toDS()
    mroDS
  }
  
  def mainregionRDD(sparkSession : SparkSession, path:String): scala.collection.Map[String, String] = {
    val rowsRDD = sparkSession.sparkContext.textFile(path)
    val mainregioneciRDD = rowsRDD.map(row => row.split(","))
                    .map{col => (col(0),col(1))}.collectAsMap()
    mainregioneciRDD
  }
  def mainregionRDD2(sparkSession : SparkSession, path:String) = {
    val rowsRDD = sparkSession.sparkContext.textFile(path)
    val mainregioneciRDD = rowsRDD.map(row => row.split(","))
                    .map{col => (col(0),col(1))}//.collectAsMap()
    mainregioneciRDD.count()
  }
  /*
   * 欧氏距离
   */
  def euclidean(x: Array[Double], y: Array[Double]) :Double= {
    var distance = 0.0;
    if(x.length == y.length){
        for (i <- 0 until x.length) {
          var temp = Math.pow((x(i) - y(i)), 2);
          distance += temp;
        }
        Math.sqrt(distance);
    }else{
      Double.MaxValue
    }
  }
  
  def similarity(objid:String, scrsrp:String,  test_nci_list:String, test_ncirsrp_list:String, lonlat:String, 
      scrsrp_avg:String, ncci_list:String, ncrsrp_list:String): Double = {
        val test_nci_arr = test_nci_list.split("-")
        val test_ncirsrp_arr = test_ncirsrp_list.split("-").map(_.toDouble)
        val test_mrorsrp_vector:Array[Double] = new Array[Double](test_ncirsrp_arr.size+1)
        test_mrorsrp_vector(0) = scrsrp.toDouble
        for(i <- 0 until test_ncirsrp_arr.length){  
          test_mrorsrp_vector(i+1) = test_ncirsrp_arr(i)
        }  
        //println(lonlat+"-"+test_nci_arr.mkString(",")+":---"+test_mrorsrp_vector.mkString(",")) 
        
        val train_ncci_arr = ncci_list.split("-")
        val train_ncrsrp_arr = ncrsrp_list.split("-").map(_.toDouble)
        
        val train_ncrsrp_map = new scala.collection.mutable.HashMap[String,Double]()
        for(i <- 0 until train_ncci_arr.length){ 
          train_ncrsrp_map.put(train_ncci_arr(i), train_ncrsrp_arr(i))
        }
        
        val train_mrorsrp_vector:Array[Double] = new Array[Double](test_ncirsrp_arr.size+1)
        train_mrorsrp_vector(0) = scrsrp_avg.toDouble

        for(i <- 0 until test_nci_arr.length){
          val ncrsrp = train_ncrsrp_map.get(test_nci_arr(i)).getOrElse(0d)
          train_mrorsrp_vector(i+1) = ncrsrp
        } 
        //println("-zq--"+train_mrorsrp_vector.mkString(","))
        euclidean(test_mrorsrp_vector, train_mrorsrp_vector)
  }
  
  def lonlat_tag(lonlat:String):String = {
        val lon = lonlat.split("-")(0).substring(0,7)
        val lat = lonlat.split("-")(1).substring(0,6)
        var lon_lastpos = lonlat.split("-")(0).substring(7,8).toInt
        var lat_lastpos = lonlat.split("-")(1).substring(6,7).toInt
        //println(">>>>>"+lon+">>>>>"+lat+">>>>>"+lon_lastpos+">>>>>"+lat_lastpos)
        if(lon_lastpos >= 5){
          lon_lastpos = 5
        }else{
          lon_lastpos = 0
        }
        if(lat_lastpos >= 5){
          lat_lastpos = 5
        }else{
          lat_lastpos = 0
        }
        lon+lon_lastpos+"-"+lat+lat_lastpos
  }
  
  def main(args: Array[String]): Unit = {
    val testdataDir = args(0)
    val traindataDir = args(1)
    val outputDir = args(2)
    val mainregionDir = args(3)
    val conf = new SparkConf().setAppName("Spark MRO Location APP")
    
//    val testdataDir = "data/mro/*/2018031413/131"
//    val traindataDir = "data/train_res_data.txt"
//    val outputDir = "D:\\mrores1"
//    val mainregionDir = "data/mainregion.txt"
//    System.setProperty("hadoop.home.dir", "E:\\bigdata-sourcecode\\hadoop_src\\hadoop-2.7.0")
//    val conf = new SparkConf().setAppName("Spark MRO broadcast").setMaster("local[5]")
    
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    
    /*
     * 主城区小区信息
     */
    println("begin read mainregion")
    val mainobjid = mainregionRDD(sparkSession,mainregionDir)
    val mainobjid_broadcastVar = sparkSession.sparkContext.broadcast(mainobjid)
    
    println("mainobjid_broadcastVar num :"+mainobjid_broadcastVar.value.size+" data size: "+SizeEstimator.estimate(mainobjid_broadcastVar.value))

    /*
     * 训练数据
     */
    println("begin read traindata")
    val mmap = new scala.collection.mutable.HashMap[String, scala.collection.mutable.Set[TrainingMro]] with scala.collection.mutable.MultiMap[String, TrainingMro]
    //collect() https://stackoverflow.com/questions/35030189/spark-broadcasting-a-multimap
    val traindataRDD = sparkSession.sparkContext.textFile(traindataDir).collect()
                    .map(row => row.split(","))
                    .map{col => TrainingMro(col(0),col(1),col(2),col(3),col(4),lonlat_tag(col(1)))}
                    .map{case mro => mmap.addBinding(mro.objid, mro)}
    val broadcastVar = sparkSession.sparkContext.broadcast(mmap)
    //println("broadcastVar size:"+broadcastVar.value.size)
    println("broadcast size :"+SizeEstimator.estimate(broadcastVar.value))
    //objid,scrsrp_avg,ncci_list,ncrsrp_list
    //106954753, 119.2738-26.0852, 49.3, 106986241-62327940, 44.3-33.7
    //println(traindataRDD.size+"<<>>"+mmap.size+"-->>"+broadcastVar.value)
    
    /*
     * 测试数据
     */
    val mroDS = rddToMroDFCase(sparkSession,testdataDir,mainobjid_broadcastVar)
    mroDS.createOrReplaceTempView("mro_testdata")

    val mrotest1DF = sparkSession.sql("select objid,scrsrp,timestamp,s1apid,concat_ws('-', collect_list(ncci)) as test_ncci_list,concat_ws('-', collect_list(ncrsrp)) as test_ncrsrp_list from mro_testdata group by objid,scrsrp,timestamp,s1apid")
    println("input mro group by size: "+mrotest1DF.count())
    val mrotest1DFfilternull = mrotest1DF.filter(!mrotest1DF("test_ncci_list").contains("null")) 
    println("input mro group by size after filter null: "+mrotest1DF.count())

    //mrotest1DFfilternull.show(false)
    //println("mrotest1DF size :"+SizeEstimator.estimate(mrotest1DF))
    //println("mrotest1DFfilternull size:"+mrotest1DFfilternull.count())//86106479
    println("end read testdata")

    /*
    val res = mrotest1DFfilternull.rdd.map{
              row => 
                val arrayBuffer = ArrayBuffer[(String,Double,Long,String,String,String,String,String,String,Double,String,String)]()
                for(train_mro <- broadcastVar.value.get(row.getString(0)).getOrElse(null)){
                  val train_ncilist = train_mro.ncci_list.split("-").toSet
                  val testncilist = row.getString(6).split("-").toSet
                  //println(train_ncilist.toString()+"---"+testncilist.toString()+"---"+testncilist.subsetOf(train_ncilist).toString())
                  if(testncilist.subsetOf(train_ncilist)){
                    arrayBuffer.+= ((row.getString(0),row.getDouble(1),row.getLong(2),row.getString(3),row.getString(4),row.getString(5),row.getString(6),row.getString(7), train_mro.lonlat, train_mro.scrsrp, train_mro.ncci_list, train_mro.ncrsrp_list))
                  }
                }
              arrayBuffer
            }
    */
    val res = mrotest1DFfilternull.rdd.mapPartitions(
          iter  => {
            //objid, scrsrp, timestamp, s1apid, lon, lat, lonlat
            val arrayBuffer = ArrayBuffer[(String, String, String, String, String)]()
            iter.foreach{//.map
              case row =>{//row=>objid,scrsrp,timestamp,s1apid, test_ncci_list,test_ncrsrp_list
                try{
                  val mm = new scala.collection.mutable.HashMap[String, scala.collection.mutable.Set[String]] with scala.collection.mutable.MultiMap[String, String]
                  if(broadcastVar.value.getOrElse(row.getString(0),null)!=null){
                    for(train_mro <- broadcastVar.value.getOrElse(row.getString(0),null)){
                      if(train_mro != null){
                          val train_ncilist = train_mro.ncci_list.split("-").toSet
                          val testncilist = row.getString(4).split("-").toSet
                          //println(train_ncilist.toString()+"---"+testncilist.toString()+"---"+testncilist.subsetOf(train_ncilist).toString())
                          if(testncilist.subsetOf(train_ncilist)){
        //                    (objid:String, scrsrp:String,  test_nci_list:String, test_ncirsrp_list:String, lonlat:String, 
        //      scrsrp_avg:Double, ncci_list:String, ncrsrp_list:String):
                            val sim = similarity(row.getString(0), row.getString(1), row.getString(4), row.getString(5), train_mro.lonlat, 
                                        train_mro.scrsrp, train_mro.ncci_list, train_mro.ncrsrp_list)
                            //val grid_tag = lonlat_tag(train_mro.lonlat)
                            //[row]: 0-objid, 1-scrsrp, 2-timestamp, 3-s1apid, 4-lon, 5-lat, 6-test_ncci_list, 7-test_ncrsrp_list
                            //train_mro.lonlat, train_mro.scrsrp, train_mro.ncci_list, train_mro.ncrsrp_list
        //                    arrayBuffer.+= ((row.getString(0),row.getDouble(1),row.getLong(2),row.getString(3),row.getString(4),row.getString(5)
        //                        , train_mro.lonlat, sim, grid_tag))
                            //(119.2765-26.0855, [119.2769-26.0859|11.7153, 119.2766-26.0855|8.0777, 119.2746-26.0852|20.4266])
                            mm.addBinding(train_mro.grid_tag, train_mro.lonlat+"|"+sim)
                          }
                      }
                    }
                  }
                  //println("mm"+mm.toString()+"--"+row.getString(0))
                  val grid2lonlat_list = scala.collection.mutable.ListBuffer.empty[(Double, String)]
                 
                  var size = 0;
                  for(ele <- mm){
                    if(ele._2.size > size){
                      grid2lonlat_list.clear()
                      for(lonlat <- ele._2){
                        grid2lonlat_list += (lonlat.split("\\|")(1).toDouble -> lonlat.split("\\|")(0))
                      }
                      size = ele._2.size
                    }else if(ele._2.size == size){
                      for(lonlat <- ele._2){
                        grid2lonlat_list += (lonlat.split("\\|")(1).toDouble -> lonlat.split("\\|")(0))
                      }
                    }
                  }
                  //grid2lonlat_list: [(20.4266,119.2746-26.0852), (8.0777,119.2766-26.0855), (11.7153,119.2769-26.0859)]
                  var avg_lon = 0d
                  var avg_lat = 0d
                  if(grid2lonlat_list.size !=0){
                      avg_lon = grid2lonlat_list.map(_._2.split("-")(0).toDouble).sum / grid2lonlat_list.size
                      avg_lat = grid2lonlat_list.map(_._2.split("-")(1).toDouble).sum / grid2lonlat_list.size
                      arrayBuffer.+= ((row.getString(0),row.getString(1),row.getString(2),row.getString(3)//,row.getString(4),row.getString(5)
                          , avg_lon.formatted("%.4f")+"-"+avg_lat.formatted("%.4f")))
                  }else{
                      arrayBuffer.+= ((row.getString(0),row.getString(1),row.getString(2),row.getString(3)//,row.getString(4),row.getString(5)
                          ,""))
                  }
                  
                }
                catch {
                  case e: Exception => {//e.printStackTrace()
                    //None
                  }
                }
              }
            }
            arrayBuffer.iterator
          }//, preservesPartitioning = true
    )
//    res.foreach(println)
    
    import sparkSession.implicits._
    val resDF = res.toDF()
    val saveOptions = Map("header" -> "false", "path" -> outputDir)
    resDF//.coalesce(1)
    .write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()

//    res.saveAsTextFile("d:\\a\\c")
//    for(ele <- broadcastVar.value){
//      println(ele._1+" -> "+ele._2.size+" -> "+ele._2)
//    }
  }
}