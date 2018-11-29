package com.chen.spark

import org.apache.spark.sql.SparkSession;

/**
  * Created by: ccong 
  * Date: 18/11/29 下午9:00
  */
object SparkRddActionApp {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("SparkRddActionApp").master("local").getOrCreate()
    val dataNumber = sc.sparkContext.parallelize(Array(1,2,3,3))
    //calculate the sum for the element with the same type
    val result = dataNumber.reduce((x,y) => x+y)
    println("reduce result : "+result)

    val takeNumber = dataNumber.take(3)
    println("take(n)")
    takeNumber.foreach(println)

    //sort the rdd and return the top n data
    /*
    * 3
    * 3
    * 2
    * */
    val topNumber = dataNumber.top(3)
    println("top(n)")
    topNumber.foreach(println)
    sc.stop()
  }
}
