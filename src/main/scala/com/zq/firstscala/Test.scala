package com.zq.firstscala

import com.google.common.collect.ArrayListMultimap
import scala.collection.immutable.TreeMap
import scala.util.Try

object Test {
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
  
  def main(args: Array[String]) {
    val colors1 = Map(
      "red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F")
    val colors2 = Map(
      "blue" -> "#0033FF",
      "yellow" -> "#FFFF00",
      "red" -> "#110000")

    //  ++ 作为运算符
    var colors = colors1 ++ colors2
    println("colors1 ++ colors2 : " + colors)

    //  ++ 作为方法
    colors = colors1.++(colors2)
    println("colors1.++(colors2)) : " + colors)

    val myMultimap = ArrayListMultimap.create[String,String]()
    // 添加键值对
    myMultimap.put("Fruits", "Apple"); 
   //给Fruits元素添加另一个元素 
    myMultimap.put("Fruits", "Apple1");  
    myMultimap.put("Fruits", "Pear");  
    myMultimap.put("Vegetables", "Carrot");  
    myMultimap.put("Fruits", "Apple2");  
    //System.out.println(myMultimap.get("Fruits")); 
    
    import collection.mutable.{ HashMap, MultiMap, Set }

    // to create a `MultiMap` the easiest way is to mixin it into a normal
    // `Map` instance
    val mm = new HashMap[Int, Set[String]] with MultiMap[Int, String]
    
    // to add key-value pairs to a multimap it is important to use
    // the method `addBinding` because standard methods like `+` will
    // overwrite the complete key-value pair instead of adding the
    // value to the existing key
    mm.addBinding(1, "a")
    mm.addBinding(2, "b")
    mm.addBinding(1, "c")
    println(mm.toString())
    
    println(euclidean(Array(42d,30d),Array(44d,10d)))
    
    val studentInfo= scala.collection.mutable.HashMap("john" -> Set("j1","j2"), "stephen" ->  Set("ss1","ss2"),"lucy" ->  Set("ll1","ll2"))
    //studentInfo.getOrElse("johns",null).foreach(println(_))
    if(studentInfo.getOrElse("johns",null)!=null){
          for(train_mro <- studentInfo.get("johns")){
      if(train_mro!=null){
              println(train_mro)

      }
    }
    }

    val in = List("1", "2", "3", "abc")

  val out1 = in.map(a => Try(a.toInt))
  val results = out1.filter(_.isSuccess)
  }
}