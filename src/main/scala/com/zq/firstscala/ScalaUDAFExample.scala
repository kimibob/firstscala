package com.zq.firstscala

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.LongType
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType

object ScalaUDAFExample {
  private class SumProductAggregateFunction extends UserDefinedAggregateFunction {
    // Define the UDAF input and result schema's
    //输入参数的数据类型定义  
    def inputSchema: StructType = // Input  = (Double price, Long quantity)
      new StructType().add("price", DoubleType).add("quantity", LongType)
    //聚合的中间过程中产生的数据的数据类型定义 
    def bufferSchema: StructType = // Output = (Double total)
      new StructType().add("total", DoubleType)
    //聚合结果的数据类型定义  
    def dataType: DataType = DoubleType
    //一致性检验，如果为true,那么输入不变的情况下计算的结果也是不变的。
    def deterministic: Boolean = true // true: our UDAF's output given an input is deterministic

    //设置聚合中间buffer的初始值，但需要保证这个语义：两个初始buffer调用下面实现的merge方法后也应该为初始buffer。  
    //即如果你初始值是1，然后你merge是执行一个相加的动作，两个初始buffer合并之后等于2，不会等于初始buffer了。这样的初始值就是有问题的，所以初始值也叫"zero value" 
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0.0) // Initialize the result to 0.0
    }

    //用输入数据input更新buffer值,类似于combineByKey
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getDouble(0) // Intermediate result to be updated
      val price = input.getDouble(0) // First input parameter
      val qty = input.getLong(1) // Second input parameter
      buffer.update(0, sum + (price * qty)) // Update the intermediate result
    }
    // Merge intermediate result sums by adding them
    //合并两个buffer,将buffer2合并到buffer1.在合并两个分区聚合结果的时候会被用到,类似于reduceByKey  
    //这里要注意该方法没有返回值，在实现的时候是把buffer2合并到buffer1中去，你需要实现这个合并细节。 
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    }
    // THe final result will be contained in 'buffer'
    //计算并返回最终的聚合结果  
    def evaluate(buffer: Row): Any = {
      buffer.getDouble(0)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Scala UDAF Example").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("WARN")
    val testDF = sqlContext.read.json("data/inventory.json")
    testDF.registerTempTable("inventory")
    // Register the UDAF with our SQLContext
    sqlContext.udf.register("SUMPRODUCT", new SumProductAggregateFunction)

    sqlContext.sql("SELECT Make, SUMPRODUCT(RetailValue,Stock) AS InventoryValuePerMake FROM inventory GROUP BY Make").show()
  }
}