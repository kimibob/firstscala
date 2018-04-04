package com.zq.firstscala

/**
 * @author ${user.name}
 */
object App {
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
    val a =new distance();
    println(distance.evaluate(26.1111, 119.2222, 26.1111, 119.2222));
  }

}
