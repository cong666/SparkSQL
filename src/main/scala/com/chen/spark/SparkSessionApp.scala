package com.chen.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by: ccong 
  * Date: 18/11/27 下午10:16
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    var sc = SparkSession.builder().appName("SparkSessionApp").master("local").getOrCreate()
    val peopleDF = sc.read.format("json").load("/Users/ccong/Documents/J2EEProject/SparkTestData/people.json")
    peopleDF.printSchema()
    sc.stop()
  }
}
