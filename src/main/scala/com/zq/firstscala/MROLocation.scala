package com.zq.firstscala

import java.util.HashSet

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import scala.util.control.Breaks
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.DataTypes
import com.google.common.collect.ArrayListMultimap
import java.util.TreeMap

/*
 * mro 原始数据定位
 */
object MROLocation {
  case class Mro(objid: String,timestamp: String, s1apid: String, scrsrp: Double, ncci: String, ncrsrp: Double )
                  
  case class TrainingMro(objid: String, scrsrp: Double, ncci: String, ncrsrp: Double, lon: Double, lat: Double)
  
  def rddToMroDFCase(sparkSession : SparkSession, path:String):Dataset[Mro] = {
    val rowsRDD = sparkSession.sparkContext.textFile(path)
    val mroRDD = rowsRDD.map(row => row.split(",")).filter(_.size == 6)
                    .map{col => Mro(col(0),col(1),col(2),col(3).toDouble,col(4),col(5).toDouble)}
       //导入隐式操作，否则RDD无法调用toDF方法
    import sparkSession.implicits._
    val mroDS = mroRDD.toDS()
    mroDS

  }

  def rddToTrainingMroDFCase(sparkSession : SparkSession, path:String):Dataset[TrainingMro] = {
    val rowsRDD = sparkSession.sparkContext.textFile(path)
    val mroRDD = rowsRDD.map(row => row.split(",")).filter(_.size == 6)
                        .map{col => TrainingMro(col(0),col(1).toDouble,col(2),col(3).toDouble,col(4).toDouble,col(5).toDouble)}
    //导入隐式操作，否则RDD无法调用toDF方法
    import sparkSession.implicits._
    val mroDS = mroRDD.toDS()
    mroDS
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
  
  /*
   * 栅格分类
   */
  def fun_lonlat_class(lonlat: String) :String= {
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
    val outputDir= args(2)
    val conf = new SparkConf().setAppName("Spark MRO Location APP")
//    val testdataDir="data/testdatazq"
//    val traindataDir = "data/traindatazq"
//    val outputDir="D:\\mrores1"
//    System.setProperty("hadoop.home.dir", "E:\\bigdata-sourcecode\\hadoop_src\\hadoop-2.7.0")
//    val conf = new SparkConf().setAppName("Spark sql test").setMaster("local[2]")
    
    /*
     * 从Spark2.0以上版本开始，Spark使用全新的SparkSession接口替代Spark1.6中的SQLContext及HiveContext接口来实现其对数据加载、转换、处理等功能。
     * SparkSession实现了SQLContext及HiveContext所有功能。
     */
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    //通过代码的方式,设置Spark log4j的级别
    sparkSession.sparkContext.setLogLevel("WARN")
    
    /*
     * 从各种数据源创建DataFrame
    val jsonDF = sparkSession.read.json("data/people.json")
    jsonDF.printSchema()
    
    val parquetDF = sparkSession.read.parquet("data/users.parquet")
    parquetDF.printSchema()
    
    val csvfile = sparkSession.read.csv("data/testdatazq")
    csvfile.printSchema()
		*/
    import org.apache.spark.sql.functions._
    val ncilt6 = udf {(test_nci_list:String) => {
        val a = test_nci_list.split("-").size
        a < 6
      }
    }
    /*
     * 测试数据
     */
    val mroDS = rddToMroDFCase(sparkSession,testdataDir)
    mroDS.createOrReplaceTempView("mro_testdata")
    //sparkSession.sql("select * from mro_testdata").show() 

    val mrotest1DF = sparkSession.sql("select objid,scrsrp,timestamp,s1apid,concat_ws('-', collect_list(ncci)) as ncci_list,concat_ws('-', collect_list(cast(ncrsrp as string))) as ncrsrp_list from mro_testdata group by objid,scrsrp,timestamp,s1apid")
    //mrotest1DF.show(false)
    val mrotest1DFfilternull = mrotest1DF.filter(!mrotest1DF("ncci_list").contains("null")).filter(ncilt6(col("ncci_list")))
      
    mrotest1DFfilternull.createOrReplaceTempView("mro_testdata_combine")
//    val mrotest2DF = sparkSession.sql("select * from mro_testdata_combine")
//    mrotest2DF.persist()
    //mrotest2DF.show(false)
    //println("----------------------------test>>>>>>>>>>>>>"+mrotest1DFfilternull.count())

    /*
     * 训练数据
     */
    val trainMroDS = rddToTrainingMroDFCase(sparkSession,traindataDir)
    //sparkSession.sql("select * from mro_traindata").show() 
    trainMroDS.createOrReplaceTempView("mro_traindata")
    val mrotrain1DF = sparkSession.sql("select objid, concat(rpad(lon,8,'0'),'-',rpad(lat,7,'0')) as lonlat, concat_ws('-', collect_list(cast(scrsrp as string))) as scrsrp_list,"+
             "concat_ws('-', collect_list(ncci)) as ncci_list, concat_ws('-', collect_list(cast(ncrsrp as string))) as ncrsrp_list from mro_traindata where ncci!='null' group by lon,lat,objid")
    mrotrain1DF.createOrReplaceTempView("mro_traindata_combine")
    //mrotrain1DF.persist()
    //println("----------------------------train>>>>>>>>>>>>>"+mrotrain1DF.count())

    //mrotrain1DF.show(3,false)
    
//    val objid2grid = sparkSession.sql("select distinct objid, lonlat from mro_traindata_combine")
//    objid2grid.createOrReplaceTempView("tab_objid2grid")
//    objid2grid.show(3,false)
    
    val mrotest_objid2grid_mrotrain = sparkSession.sql("select t1.objid,t1.scrsrp,t1.timestamp,t1.s1apid,t1.ncci_list as test_nci_list,t1.ncrsrp_list as test_ncirsrp_list,t2.lonlat,t2.scrsrp_list,t2.ncci_list,t2.ncrsrp_list"+
                                      " from mro_testdata_combine t1 inner join mro_traindata_combine t2 on t1.objid = t2.objid")
    println("----------------------------mrotest_objid2grid_mrotrain>>>>>>>>>>>>>"+mrotest_objid2grid_mrotrain.count())


    val scRsrpOverRange = udf {(scrsrp:Double, scrsrp_list:String) => {
        val loop = new Breaks;
        var isInRange = true;
        val scrsrp_array = scrsrp_list.split("-")
        loop.breakable {
          for (elem <- scrsrp_array){
            if(Math.abs(scrsrp-elem.toDouble) > 2){
              isInRange = false
              loop.break
            }
          }
        }
        isInRange
      }
    }
    val nciInList = udf {(test_nci_list:String, train_ncci_list:String) => {
        val a = test_nci_list.split("-").toSet
        val b = train_ncci_list.split("-").toSet
        a.subsetOf(b)
      }
    }
    val similarity = udf {(objid:String, scrsrp:String,  test_nci_list:String, test_ncirsrp_list:String, lonlat:String, scrsrp_list:String, ncci_list:String, ncrsrp_list:String) => {
        val test_nci_arr = test_nci_list.split("-")
        val test_ncirsrp_arr = test_ncirsrp_list.split("-").map(_.toDouble)
        val test_mrorsrp_vector:Array[Double] = new Array[Double](test_ncirsrp_arr.size+1)
        test_mrorsrp_vector(0) = scrsrp.toDouble
        for(i <- 0 until test_ncirsrp_arr.length){  
          test_mrorsrp_vector(i+1) = test_ncirsrp_arr(i)
        }  
        //println(lonlat+"-"+test_nci_arr.mkString(",")+":---"+test_mrorsrp_vector.mkString(",")) 
        
        val train_scrsrp_arr = scrsrp_list.split("-").map(_.toDouble)
        val avg_train_scrsrp = train_scrsrp_arr.sum / train_scrsrp_arr.length
        
        val train_ncci_arr = ncci_list.split("-")
        val train_ncrsrp_arr = ncrsrp_list.split("-").map(_.toDouble)
        val train_ncrsrp_map = ArrayListMultimap.create[String,Double]()
        for(i <- 0 until train_ncci_arr.length){ 
          train_ncrsrp_map.put(train_ncci_arr(i), train_ncrsrp_arr(i)) 
        }
        //System.out.println("-->>>"+train_ncrsrp_map); 
        
        val train_mrorsrp_vector:Array[Double] = new Array[Double](test_ncirsrp_arr.size+1)
        train_mrorsrp_vector(0) = avg_train_scrsrp
        import collection.JavaConverters._
        for(i <- 0 until test_nci_arr.length){
          val rsrp_list = train_ncrsrp_map.get(test_nci_arr(i)).asScala.toArray
          train_mrorsrp_vector(i+1) = rsrp_list.sum / rsrp_list.length
        } 
        //println("-zq--"+train_mrorsrp_vector.mkString(","))
        euclidean(test_mrorsrp_vector, train_mrorsrp_vector)
      }
    }

    val lonlat_tag = udf {(lonlat:String) => {
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
    }
    val saveOptions = Map("header" -> "false", "path" -> outputDir)    
    val res = mrotest_objid2grid_mrotrain//.filter(scRsrpOverRange(col("scrsrp"), col("scrsrp_list").cast("String")))
        .filter(nciInList(col("test_nci_list"), col("ncci_list")))
    //res.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    res.coalesce(10).rdd.saveAsTextFile(outputDir)
    //res.cache()
    //println("----------------------------res part num>>>>>>>>>>>>>"+res.rdd.getNumPartitions)
    //val res1 = res.withColumn("sim",similarity(col("objid"), col("scrsrp"), col("test_nci_list"), col("test_ncirsrp_list"), col("lonlat"), col("scrsrp_list"), col("ncci_list"), col("ncrsrp_list")))
    //println("----------------------------res1 part num>>>>>>>>>>>>>"+res1.rdd.getNumPartitions)
    //val res2 = res1.withColumn("lonlat_class", lonlat_tag(col("lonlat")))
    //println("----------------------------res2 part num>>>>>>>>>>>>>"+res2.rdd.getNumPartitions)
    //val res3 = res2.select(col("objid"), col("scrsrp"), col("timestamp"), col("s1apid"), col("lonlat"), col("sim"), col("lonlat_class"))
    //println("----------------------------res3 part num>>>>>>>>>>>>>"+res3.rdd.getNumPartitions)


    //sparkSession.sqlContext.udf.register("Classify", new ClassifyAggregateFunction, StringType)
    //val classify = new ClassifyAggregateFunction
    //res3.show(100,false)
    //val res4 = res3.groupBy(col("objid"), col("scrsrp"), col("timestamp"), col("s1apid")).agg(classify(col("lonlat"), col("sim")).as("location_list"))
    //println("----------------------------res4 part num>>>>>>>>>>>>>"+res4.rdd.getNumPartitions)
    //res4.show(100,false)
 
    //mrotest_objid2grid_mrotrain.show()
    //res4//.coalesce(1)
    //println("----------------------------res4 count:"+res4.count()+"------------")
    //.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()

    
  }
  
  private class ClassifyAggregateFunction extends UserDefinedAggregateFunction {

    def inputSchema: StructType = 
      new StructType().add("lonlat", StringType).add("sim", StringType)
    def bufferSchema: StructType = 
      new StructType().add("buffer", MapType(StringType, StringType))
    def dataType: DataType = StringType
    def deterministic: Boolean = true // true: our UDAF's output given an input is deterministic

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, Map[String, String]())
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val newBuffer0 = buffer.getMap[String, String](0)
      buffer.update(0, newBuffer0 + (input.getString(0) -> input.getString(1)))
      
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getMap[String, String](0) ++ buffer2.getMap[String, String](0))
    }
    
    def evaluate(buffer: Row): Any = {
      val sourcemap = buffer.getMap[String, String](0)//119.2758-26.0851(lonlat) -> 6.412812830 sim
      val mm = new scala.collection.mutable.HashMap[String, scala.collection.mutable.Set[String]] with scala.collection.mutable.MultiMap[String, String]
      val map = sourcemap.map{case(lonlat, sim) => 
        mm.addBinding(fun_lonlat_class(lonlat), lonlat)
      }
      //mm
      //119.2755-26.0850 -> 119.2758-26.0851,119.2756-26.0851,119.2759-26.0854
      //119.2760-26.0855 -> 119.2762-26.0855,119.2761-26.0857
      //119.2750-26.0855 -> 119.2751-26.0856,119.2753-26.0856,119.2753-26.0857
      var treemap_grid = scala.collection.immutable.TreeMap.empty[Double, String]
      var size = 0;
      for(ele <- mm){
        if(ele._2.size > size){
          treemap_grid = scala.collection.immutable.TreeMap.empty[Double, String]
          for(lonlat <- ele._2){
            treemap_grid += (sourcemap.get(lonlat).getOrElse("0").toDouble -> lonlat)
          }
          size = ele._2.size
        }else if(ele._2.size == size){
          for(lonlat <- ele._2){
            treemap_grid += (sourcemap.get(lonlat).getOrElse("0").toDouble -> lonlat)
          }
        }
        //println(ele.toString()+ " ssize:"+ele._2.size)
      }
//      val a =  treemap_grid.get(treemap_grid.firstKey)
//      a
      var lon=0d;
      var lat=0d;
      var num=0;
      val a = treemap_grid.values.map(lonlat => (lonlat.split("-")(0).toDouble, lonlat.split("-")(1).toDouble))
      if(a.size !=0)
      {
        val avg_lon = a.map(_._1).sum / a.size
        val avg_lat = a.map(_._2).sum / a.size
        avg_lon.formatted("%.4f")+"-"+avg_lat.formatted("%.4f")
      }
      
    }
  }
  
}