package com.zq.firstscala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
    def main(args: Array[String]) {
        val inputFile =  "d://a.txt"
        val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
        val sc = new SparkContext(conf)
                val textFile = sc.textFile(inputFile)
                val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
                wordCount.foreach(println)       
    }
}